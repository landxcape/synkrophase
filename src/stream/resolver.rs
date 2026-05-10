use std::path::PathBuf;
use std::sync::Arc;
use crate::error::Result;
use crate::stream::server::MediaServer;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedStream {
    pub url: String,
}

pub struct MediaRouter {
    server: Arc<MediaServer>,
}

impl MediaRouter {
    pub fn new(server: Arc<MediaServer>) -> Self {
        Self { server }
    }

    pub async fn resolve(&self, input: &str) -> Result<ResolvedStream> {
        let path = PathBuf::from(input);
        if path.exists() && path.is_file() {
            // Local file - host it
            self.server.host_file(path);
            Ok(ResolvedStream { url: self.server.get_url() })
        } else {
            // Assume it's a direct URL
            Ok(ResolvedStream { url: input.to_string() })
        }
    }
}
