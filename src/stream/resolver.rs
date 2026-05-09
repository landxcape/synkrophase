use std::path::PathBuf;

use tokio::process::Command;

use crate::error::{Result, SynkroError};
use crate::stream::bootstrap::parse_expiry;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedStream {
    pub url: String,
    pub expires_at: Option<u64>,
}

pub struct StreamResolver {
    ytdlp_path: PathBuf,
}

impl StreamResolver {
    pub fn new(ytdlp_path: PathBuf) -> Self {
        Self { ytdlp_path }
    }

    pub async fn resolve(&self, youtube_url: &str) -> Result<ResolvedStream> {
        let output = self
            .run_command([
                "-f",
                "bestaudio[protocol^=http]/best[protocol^=http]",
                "--get-url",
                youtube_url,
            ])
            .await?;
        let url = first_line(&output)?;

        Ok(ResolvedStream {
            expires_at: parse_expiry(url),
            url: url.to_string(),
        })
    }

    pub async fn get_title(&self, youtube_url: &str) -> Result<String> {
        let output = self.run_command(["--get-title", youtube_url]).await?;
        Ok(first_line(&output)?.to_string())
    }

    async fn run_command<const N: usize>(&self, args: [&str; N]) -> Result<String> {
        let output = Command::new(&self.ytdlp_path)
            .args(["--cookies-from-browser", "chrome"])
            .args(args)
            .output()
            .await
            .map_err(SynkroError::Network)?;

        if !output.status.success() {
            return Err(SynkroError::StreamResolution(
                String::from_utf8_lossy(&output.stderr).trim().to_string(),
            ));
        }

        String::from_utf8(output.stdout)
            .map_err(|err| SynkroError::StreamResolution(err.to_string()))
    }
}

fn first_line(output: &str) -> Result<&str> {
    output
        .lines()
        .find(|line| !line.trim().is_empty())
        .map(str::trim)
        .ok_or_else(|| SynkroError::StreamResolution("yt-dlp produced empty output".into()))
}
