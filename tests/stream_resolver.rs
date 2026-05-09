use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;

use synkrophase::stream::resolver::StreamResolver;
use uuid::Uuid;

fn temp_file(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("synkro-{name}-{}", Uuid::new_v4()))
}

fn write_script(contents: &str) -> PathBuf {
    let path = temp_file("yt-dlp.sh");
    fs::write(&path, contents).unwrap();
    let mut perms = fs::metadata(&path).unwrap().permissions();
    perms.set_mode(0o755);
    fs::set_permissions(&path, perms).unwrap();
    path
}

#[tokio::test]
async fn resolve_returns_url_and_expiry() {
    let script = write_script(
        "#!/bin/sh\nprintf 'https://rr2---sn.googlevideo.com/videoplayback?expire=1712345678\\n'",
    );
    let resolver = StreamResolver::new(script);

    let resolved = resolver
        .resolve("https://youtube.com/watch?v=test")
        .await
        .unwrap();

    assert_eq!(
        resolved.url,
        "https://rr2---sn.googlevideo.com/videoplayback?expire=1712345678"
    );
    assert_eq!(resolved.expires_at, Some(1_712_345_678));
}

#[tokio::test]
async fn get_title_returns_trimmed_output() {
    let script = write_script(
        "#!/bin/sh\nfor arg in \"$@\"; do\n  if [ \"$arg\" = \"--get-title\" ]; then\n    printf 'Test Title\\n'\n    exit 0\n  fi\ndone\nprintf 'https://example.com\\n'\n",
    );
    let resolver = StreamResolver::new(script);

    let title = resolver
        .get_title("https://youtube.com/watch?v=test")
        .await
        .unwrap();

    assert_eq!(title, "Test Title");
}
