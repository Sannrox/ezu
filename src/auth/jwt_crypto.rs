//! JWT signatures through jsonwebtoken's `CryptoProvider`, backed by `ring`.
//!
//! jsonwebtoken's `rust_crypto` backend still depends on `p256`/`p384` 0.13
//! (`crypto-bigint` 0.5, RUSTSEC-2026-0387). Ezu already uses `ring` for TLS,
//! so JWT verification uses the same library and drops that advisory.

use jsonwebtoken::crypto::{CryptoProvider, JwtSigner, JwtVerifier, KeyUtils};
use jsonwebtoken::errors::{ErrorKind, Result, new_error};
use jsonwebtoken::signature::{Signer, Verifier};
use jsonwebtoken::{Algorithm, AlgorithmFamily, DecodingKey, DecodingKeyKind, EncodingKey};
use ring::hmac;
use ring::rand::SystemRandom;
use ring::signature::{
    self, ECDSA_P256_SHA256_FIXED, ECDSA_P256_SHA256_FIXED_SIGNING, ECDSA_P384_SHA384_FIXED,
    ECDSA_P384_SHA384_FIXED_SIGNING, ED25519, EcdsaKeyPair, Ed25519KeyPair,
    RSA_PKCS1_2048_8192_SHA256, RSA_PKCS1_2048_8192_SHA384, RSA_PKCS1_2048_8192_SHA512,
    RSA_PKCS1_SHA256, RSA_PKCS1_SHA384, RSA_PKCS1_SHA512, RSA_PSS_2048_8192_SHA256,
    RSA_PSS_2048_8192_SHA384, RSA_PSS_2048_8192_SHA512, RSA_PSS_SHA256, RSA_PSS_SHA384,
    RSA_PSS_SHA512, RsaKeyPair, RsaPublicKeyComponents, UnparsedPublicKey,
};

fn sig_err<E>(_: E) -> jsonwebtoken::signature::Error {
    jsonwebtoken::signature::Error::new()
}

fn rsa_key_pair(bytes: &[u8]) -> std::result::Result<RsaKeyPair, jsonwebtoken::signature::Error> {
    RsaKeyPair::from_der(bytes)
        .or_else(|_| RsaKeyPair::from_pkcs8(bytes))
        .map_err(sig_err)
}

fn try_sign_rsa(
    encoding: &'static dyn signature::RsaEncoding,
    key: &EncodingKey,
    msg: &[u8],
) -> std::result::Result<Vec<u8>, jsonwebtoken::signature::Error> {
    let key_pair = rsa_key_pair(key.as_bytes())?;
    let mut signature = vec![0; key_pair.public().modulus_len()];
    key_pair
        .sign(encoding, &SystemRandom::new(), msg, &mut signature)
        .map_err(sig_err)?;
    Ok(signature)
}

fn verify_rsa(
    params: &'static signature::RsaParameters,
    key: &DecodingKey,
    msg: &[u8],
    signature: &[u8],
) -> std::result::Result<(), jsonwebtoken::signature::Error> {
    match key.kind() {
        DecodingKeyKind::SecretOrDer(bytes) => UnparsedPublicKey::new(params, bytes)
            .verify(msg, signature)
            .map_err(sig_err),
        DecodingKeyKind::RsaModulusExponent { n, e } => RsaPublicKeyComponents { n, e }
            .verify(params, msg, signature)
            .map_err(sig_err),
    }
}

macro_rules! hmac_pair {
    ($signer:ident, $verifier:ident, $alg:expr, $hmac:expr) => {
        struct $signer(hmac::Key);
        impl $signer {
            fn new(key: &EncodingKey) -> Result<Self> {
                if key.family() != AlgorithmFamily::Hmac {
                    return Err(new_error(ErrorKind::InvalidKeyFormat));
                }
                Ok(Self(hmac::Key::new($hmac, key.as_bytes())))
            }
        }
        impl Signer<Vec<u8>> for $signer {
            fn try_sign(
                &self,
                msg: &[u8],
            ) -> std::result::Result<Vec<u8>, jsonwebtoken::signature::Error> {
                Ok(hmac::sign(&self.0, msg).as_ref().to_vec())
            }
        }
        impl JwtSigner for $signer {
            fn algorithm(&self) -> Algorithm {
                $alg
            }
        }

        struct $verifier(hmac::Key);
        impl $verifier {
            fn new(key: &DecodingKey) -> Result<Self> {
                if key.family() != AlgorithmFamily::Hmac {
                    return Err(new_error(ErrorKind::InvalidKeyFormat));
                }
                Ok(Self(hmac::Key::new($hmac, key.try_get_as_bytes()?)))
            }
        }
        impl Verifier<Vec<u8>> for $verifier {
            fn verify(
                &self,
                msg: &[u8],
                signature: &Vec<u8>,
            ) -> std::result::Result<(), jsonwebtoken::signature::Error> {
                hmac::verify(&self.0, msg, signature).map_err(sig_err)
            }
        }
        impl JwtVerifier for $verifier {
            fn algorithm(&self) -> Algorithm {
                $alg
            }
        }
    };
}

hmac_pair!(
    Hs256Signer,
    Hs256Verifier,
    Algorithm::HS256,
    hmac::HMAC_SHA256
);
hmac_pair!(
    Hs384Signer,
    Hs384Verifier,
    Algorithm::HS384,
    hmac::HMAC_SHA384
);
hmac_pair!(
    Hs512Signer,
    Hs512Verifier,
    Algorithm::HS512,
    hmac::HMAC_SHA512
);

macro_rules! ecdsa_pair {
    ($signer:ident, $verifier:ident, $alg:expr, $sign:expr, $verify:expr) => {
        struct $signer(EcdsaKeyPair);
        impl $signer {
            fn new(key: &EncodingKey) -> Result<Self> {
                if key.family() != AlgorithmFamily::Ec {
                    return Err(new_error(ErrorKind::InvalidKeyFormat));
                }
                Ok(Self(
                    EcdsaKeyPair::from_pkcs8($sign, key.as_bytes(), &SystemRandom::new())
                        .map_err(|_| ErrorKind::InvalidEcdsaKey)?,
                ))
            }
        }
        impl Signer<Vec<u8>> for $signer {
            fn try_sign(
                &self,
                msg: &[u8],
            ) -> std::result::Result<Vec<u8>, jsonwebtoken::signature::Error> {
                self.0
                    .sign(&SystemRandom::new(), msg)
                    .map(|sig| sig.as_ref().to_vec())
                    .map_err(sig_err)
            }
        }
        impl JwtSigner for $signer {
            fn algorithm(&self) -> Algorithm {
                $alg
            }
        }

        struct $verifier(DecodingKey);
        impl $verifier {
            fn new(key: &DecodingKey) -> Result<Self> {
                if key.family() != AlgorithmFamily::Ec {
                    return Err(new_error(ErrorKind::InvalidKeyFormat));
                }
                Ok(Self(key.clone()))
            }
        }
        impl Verifier<Vec<u8>> for $verifier {
            fn verify(
                &self,
                msg: &[u8],
                signature: &Vec<u8>,
            ) -> std::result::Result<(), jsonwebtoken::signature::Error> {
                UnparsedPublicKey::new($verify, self.0.try_get_as_bytes().map_err(sig_err)?)
                    .verify(msg, signature)
                    .map_err(sig_err)
            }
        }
        impl JwtVerifier for $verifier {
            fn algorithm(&self) -> Algorithm {
                $alg
            }
        }
    };
}

ecdsa_pair!(
    Es256Signer,
    Es256Verifier,
    Algorithm::ES256,
    &ECDSA_P256_SHA256_FIXED_SIGNING,
    &ECDSA_P256_SHA256_FIXED
);
ecdsa_pair!(
    Es384Signer,
    Es384Verifier,
    Algorithm::ES384,
    &ECDSA_P384_SHA384_FIXED_SIGNING,
    &ECDSA_P384_SHA384_FIXED
);

macro_rules! rsa_pair {
    ($signer:ident, $verifier:ident, $alg:expr, $sign:expr, $verify:expr) => {
        struct $signer(EncodingKey);
        impl $signer {
            fn new(key: &EncodingKey) -> Result<Self> {
                if key.family() != AlgorithmFamily::Rsa {
                    return Err(new_error(ErrorKind::InvalidKeyFormat));
                }
                Ok(Self(key.clone()))
            }
        }
        impl Signer<Vec<u8>> for $signer {
            fn try_sign(
                &self,
                msg: &[u8],
            ) -> std::result::Result<Vec<u8>, jsonwebtoken::signature::Error> {
                try_sign_rsa($sign, &self.0, msg)
            }
        }
        impl JwtSigner for $signer {
            fn algorithm(&self) -> Algorithm {
                $alg
            }
        }

        struct $verifier(DecodingKey);
        impl $verifier {
            fn new(key: &DecodingKey) -> Result<Self> {
                if key.family() != AlgorithmFamily::Rsa {
                    return Err(new_error(ErrorKind::InvalidKeyFormat));
                }
                Ok(Self(key.clone()))
            }
        }
        impl Verifier<Vec<u8>> for $verifier {
            fn verify(
                &self,
                msg: &[u8],
                signature: &Vec<u8>,
            ) -> std::result::Result<(), jsonwebtoken::signature::Error> {
                verify_rsa($verify, &self.0, msg, signature)
            }
        }
        impl JwtVerifier for $verifier {
            fn algorithm(&self) -> Algorithm {
                $alg
            }
        }
    };
}

rsa_pair!(
    Rsa256Signer,
    Rsa256Verifier,
    Algorithm::RS256,
    &RSA_PKCS1_SHA256,
    &RSA_PKCS1_2048_8192_SHA256
);
rsa_pair!(
    Rsa384Signer,
    Rsa384Verifier,
    Algorithm::RS384,
    &RSA_PKCS1_SHA384,
    &RSA_PKCS1_2048_8192_SHA384
);
rsa_pair!(
    Rsa512Signer,
    Rsa512Verifier,
    Algorithm::RS512,
    &RSA_PKCS1_SHA512,
    &RSA_PKCS1_2048_8192_SHA512
);
rsa_pair!(
    Pss256Signer,
    Pss256Verifier,
    Algorithm::PS256,
    &RSA_PSS_SHA256,
    &RSA_PSS_2048_8192_SHA256
);
rsa_pair!(
    Pss384Signer,
    Pss384Verifier,
    Algorithm::PS384,
    &RSA_PSS_SHA384,
    &RSA_PSS_2048_8192_SHA384
);
rsa_pair!(
    Pss512Signer,
    Pss512Verifier,
    Algorithm::PS512,
    &RSA_PSS_SHA512,
    &RSA_PSS_2048_8192_SHA512
);

struct EdSigner(Ed25519KeyPair);
impl EdSigner {
    fn new(key: &EncodingKey) -> Result<Self> {
        if key.family() != AlgorithmFamily::Ed {
            return Err(new_error(ErrorKind::InvalidKeyFormat));
        }
        Ok(Self(
            Ed25519KeyPair::from_pkcs8(key.as_bytes()).map_err(|_| ErrorKind::InvalidEddsaKey)?,
        ))
    }
}
impl Signer<Vec<u8>> for EdSigner {
    fn try_sign(&self, msg: &[u8]) -> std::result::Result<Vec<u8>, jsonwebtoken::signature::Error> {
        Ok(self.0.sign(msg).as_ref().to_vec())
    }
}
impl JwtSigner for EdSigner {
    fn algorithm(&self) -> Algorithm {
        Algorithm::EdDSA
    }
}

struct EdVerifier(DecodingKey);
impl EdVerifier {
    fn new(key: &DecodingKey) -> Result<Self> {
        if key.family() != AlgorithmFamily::Ed {
            return Err(new_error(ErrorKind::InvalidKeyFormat));
        }
        Ok(Self(key.clone()))
    }
}
impl Verifier<Vec<u8>> for EdVerifier {
    fn verify(
        &self,
        msg: &[u8],
        signature: &Vec<u8>,
    ) -> std::result::Result<(), jsonwebtoken::signature::Error> {
        UnparsedPublicKey::new(&ED25519, self.0.try_get_as_bytes().map_err(sig_err)?)
            .verify(msg, signature)
            .map_err(sig_err)
    }
}
impl JwtVerifier for EdVerifier {
    fn algorithm(&self) -> Algorithm {
        Algorithm::EdDSA
    }
}

fn new_signer(
    algorithm: &Algorithm,
    key: &EncodingKey,
) -> jsonwebtoken::errors::Result<Box<dyn JwtSigner>> {
    Ok(match algorithm {
        Algorithm::HS256 => Box::new(Hs256Signer::new(key)?),
        Algorithm::HS384 => Box::new(Hs384Signer::new(key)?),
        Algorithm::HS512 => Box::new(Hs512Signer::new(key)?),
        Algorithm::ES256 => Box::new(Es256Signer::new(key)?),
        Algorithm::ES384 => Box::new(Es384Signer::new(key)?),
        Algorithm::RS256 => Box::new(Rsa256Signer::new(key)?),
        Algorithm::RS384 => Box::new(Rsa384Signer::new(key)?),
        Algorithm::RS512 => Box::new(Rsa512Signer::new(key)?),
        Algorithm::PS256 => Box::new(Pss256Signer::new(key)?),
        Algorithm::PS384 => Box::new(Pss384Signer::new(key)?),
        Algorithm::PS512 => Box::new(Pss512Signer::new(key)?),
        Algorithm::EdDSA => Box::new(EdSigner::new(key)?),
        _ => return Err(new_error(ErrorKind::UnsupportedAlgorithm)),
    })
}

fn new_verifier(
    algorithm: &Algorithm,
    key: &DecodingKey,
) -> jsonwebtoken::errors::Result<Box<dyn JwtVerifier>> {
    Ok(match algorithm {
        Algorithm::HS256 => Box::new(Hs256Verifier::new(key)?),
        Algorithm::HS384 => Box::new(Hs384Verifier::new(key)?),
        Algorithm::HS512 => Box::new(Hs512Verifier::new(key)?),
        Algorithm::ES256 => Box::new(Es256Verifier::new(key)?),
        Algorithm::ES384 => Box::new(Es384Verifier::new(key)?),
        Algorithm::RS256 => Box::new(Rsa256Verifier::new(key)?),
        Algorithm::RS384 => Box::new(Rsa384Verifier::new(key)?),
        Algorithm::RS512 => Box::new(Rsa512Verifier::new(key)?),
        Algorithm::PS256 => Box::new(Pss256Verifier::new(key)?),
        Algorithm::PS384 => Box::new(Pss384Verifier::new(key)?),
        Algorithm::PS512 => Box::new(Pss512Verifier::new(key)?),
        Algorithm::EdDSA => Box::new(EdVerifier::new(key)?),
        _ => return Err(new_error(ErrorKind::UnsupportedAlgorithm)),
    })
}

static PROVIDER: CryptoProvider = CryptoProvider {
    signer_factory: new_signer,
    verifier_factory: new_verifier,
    key_utils: KeyUtils::new_unimplemented(),
};

/// Install the ring JWT provider once for this process.
pub fn install() {
    let _ = PROVIDER.install_default();
}
