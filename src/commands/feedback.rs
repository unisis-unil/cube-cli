use anyhow::{bail, Context, Result};
use reqwest::blocking::Client;
use serde::Deserialize;
use std::io::{IsTerminal, Read};

const GCS_API: &str = "https://storage.googleapis.com/storage/v1";
const MAILGUN_KEY_OBJECT: &str = "mailgun-key.json";
const MAILGUN_ENDPOINT: &str = "https://api.eu.mailgun.net/v3/mailgun.unisis.ch/messages";
const RECIPIENT: &str = "unisis@unil.ch";

#[derive(Deserialize)]
struct MailgunKey {
    api_key: String,
}

#[derive(Deserialize)]
struct GoogleUserInfo {
    email: String,
}

/// Fetch user email from Google userinfo endpoint.
fn get_user_email(token: &str) -> Result<String> {
    let client = Client::new();
    let resp = client
        .get("https://www.googleapis.com/oauth2/v3/userinfo")
        .bearer_auth(token)
        .send()
        .context("Impossible de contacter l'API Google userinfo")?;

    if !resp.status().is_success() {
        bail!(
            "Impossible de récupérer les informations utilisateur (HTTP {}). \
             Exécutez 'gcloud auth application-default login'.",
            resp.status()
        );
    }

    let info: GoogleUserInfo = resp.json().context("Réponse userinfo invalide")?;

    Ok(info.email)
}

/// Fetch the Mailgun API key from GCS bucket.
fn fetch_mailgun_key(bucket: &str, token: &str) -> Result<String> {
    let client = Client::new();
    let encoded = urlencoding::encode(MAILGUN_KEY_OBJECT);
    let url = format!("{GCS_API}/b/{bucket}/o/{encoded}?alt=media");

    let resp = client.get(&url).bearer_auth(token).send()?;

    if resp.status() == reqwest::StatusCode::NOT_FOUND {
        bail!(
            "Configuration Mailgun introuvable dans gs://{bucket}/{MAILGUN_KEY_OBJECT}. \
             Contactez l'équipe UNISIS."
        );
    }

    let key: MailgunKey = resp
        .error_for_status()?
        .json()
        .context("Format invalide pour mailgun-key.json")?;

    Ok(key.api_key)
}

/// Send feedback email via Mailgun.
fn send_mailgun(api_key: &str, from: &str, subject: &str, body: &str) -> Result<()> {
    let client = Client::new();
    let resp = client
        .post(MAILGUN_ENDPOINT)
        .basic_auth("api", Some(api_key))
        .form(&[
            ("from", from),
            ("to", RECIPIENT),
            ("subject", subject),
            ("text", body),
        ])
        .send()
        .context("Impossible de contacter l'API Mailgun")?;

    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().unwrap_or_default();
        bail!("Erreur Mailgun (HTTP {status}) : {text}");
    }

    Ok(())
}

/// Collect the feedback message from argument, stdin, or interactive prompt.
fn collect_message(message: Option<&str>) -> Result<String> {
    if let Some(msg) = message {
        return Ok(msg.to_string());
    }

    let stdin = std::io::stdin();
    if !stdin.is_terminal() {
        // Read from pipe
        let mut buf = String::new();
        stdin.lock().read_to_string(&mut buf)?;
        let trimmed = buf.trim().to_string();
        if trimmed.is_empty() {
            bail!("Message vide. Fournissez un message via l'argument ou stdin.");
        }
        return Ok(trimmed);
    }

    bail!(
        "Aucun message fourni. Utilisez :\n  \
         cube feedback \"Votre message\"\n  \
         echo \"Votre message\" | cube feedback"
    );
}

/// Run the `cube feedback` command.
pub fn run(message: Option<&str>, dev: bool) -> Result<()> {
    let body = collect_message(message)?;

    // 1. Authenticate via GCP
    eprintln!("Vérification de l'authentification GCP...");
    let token = super::sync::get_access_token()?;

    // 2. Get user identity
    let email = get_user_email(&token)?;
    eprintln!("Authentifié en tant que {email}");

    // 3. Fetch Mailgun API key from GCS
    let bucket = super::sync::bucket_for(dev);
    let api_key = fetch_mailgun_key(bucket, &token)?;

    // 4. Send feedback
    let version = env!("CARGO_PKG_VERSION");
    let subject = format!("[cube-cli v{version}] Feedback de {email}");
    let full_body = format!("De : {email}\nVersion : cube-cli v{version}\n\n{body}");

    eprintln!("Envoi du feedback...");
    send_mailgun(&api_key, &email, &subject, &full_body)?;

    eprintln!("Feedback envoyé à {RECIPIENT}. Merci !");
    Ok(())
}
