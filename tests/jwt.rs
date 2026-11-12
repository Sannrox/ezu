//! JWT verification through the ring CryptoProvider for HMAC, ECDSA and RSA.

use base64::Engine as _;
use ezu::auth::{AuthError, Authenticator, Jwt};
use ezu::entity::Namespace;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use ring::rand::SystemRandom;
use ring::signature::{
    ECDSA_P256_SHA256_FIXED_SIGNING, EcdsaKeyPair, KeyPair, RsaKeyPair, RsaPublicKeyComponents,
};

const SECRET: &[u8] = b"test-only-hmac-secret-0123456789";
const RSA_PKCS8: &[u8] = include_bytes!("fixtures/jwt-rsa-2048.pk8");

fn claims(tenant: &str) -> serde_json::Value {
    serde_json::json!({
        "sub": format!("svc-{tenant}"),
        "aud": "ezu",
        "tenant": tenant,
        "exp": 4_102_444_800u64,
    })
}

fn b64(bytes: &[u8]) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

fn hmac_case() -> (String, EncodingKey, Algorithm) {
    let k = b64(SECRET);
    let jwks = format!(r#"{{"keys":[{{"kty":"oct","kid":"k1","alg":"HS256","k":"{k}"}}]}}"#);
    (jwks, EncodingKey::from_secret(SECRET), Algorithm::HS256)
}

fn ecdsa_case(pkcs8: &[u8]) -> (String, EncodingKey, Algorithm) {
    let rng = SystemRandom::new();
    let pair = EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, pkcs8, &rng).unwrap();
    let public = pair.public_key().as_ref();
    assert_eq!(public[0], 0x04);
    let (x, y) = public[1..].split_at(32);
    let jwks = format!(
        r#"{{"keys":[{{"kty":"EC","crv":"P-256","kid":"k1","alg":"ES256","x":"{}","y":"{}"}}]}}"#,
        b64(x),
        b64(y)
    );
    (jwks, EncodingKey::from_ec_der(pkcs8), Algorithm::ES256)
}

fn rsa_case() -> (String, EncodingKey, Algorithm) {
    let pair = RsaKeyPair::from_pkcs8(RSA_PKCS8).unwrap();
    let components: RsaPublicKeyComponents<Vec<u8>> = pair.public().into();
    let jwks = format!(
        r#"{{"keys":[{{"kty":"RSA","kid":"k1","alg":"RS256","n":"{}","e":"{}"}}]}}"#,
        b64(&components.n),
        b64(&components.e)
    );
    (jwks, EncodingKey::from_rsa_der(RSA_PKCS8), Algorithm::RS256)
}

fn token(encoding: &EncodingKey, alg: Algorithm, tenant: &str) -> String {
    let mut header = Header::new(alg);
    header.kid = Some("k1".into());
    encode(&header, &claims(tenant), encoding).unwrap()
}

#[test]
fn a_jwks_accepts_tokens_for_hmac_ecdsa_and_rsa_keys() {
    let rng = SystemRandom::new();
    let es_pkcs8 = EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &rng).unwrap();
    for (jwks, encoding, alg) in [hmac_case(), ecdsa_case(es_pkcs8.as_ref()), rsa_case()] {
        let auth = Jwt::new(&jwks, "ezu", None, "tenant").unwrap();
        let identity = auth
            .authenticate(Some(&token(&encoding, alg, "tenant-a")))
            .unwrap();
        assert_eq!(identity.subject, "svc-tenant-a");
        assert_eq!(Namespace::of(&identity).as_str(), "tenant-a");
    }
}

#[test]
fn an_ecdsa_token_signed_by_another_key_is_invalid() {
    let rng = SystemRandom::new();
    let accepted = EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &rng).unwrap();
    let other = EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &rng).unwrap();
    let (jwks, _, alg) = ecdsa_case(accepted.as_ref());
    let auth = Jwt::new(&jwks, "ezu", None, "tenant").unwrap();
    let bearer = token(&EncodingKey::from_ec_der(other.as_ref()), alg, "tenant-a");
    assert_eq!(auth.authenticate(Some(&bearer)), Err(AuthError::Invalid));
}
