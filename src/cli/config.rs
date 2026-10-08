use std::env;
use std::fs;
use uuid::Uuid;

use crate::config::DeviceConfig;
use crate::error::Result;

pub fn load_device_config(name_opt: Option<String>, ephemeral: bool) -> Result<DeviceConfig> {
    let home = home::home_dir().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "Could not find home directory",
        )
    })?;
    let data_dir = home.join(".synkrophase");
    fs::create_dir_all(&data_dir)?;

    let name = name_opt
        .or_else(|| env::var("USER").ok())
        .or_else(|| env::var("USERNAME").ok())
        .unwrap_or_else(|| "User".to_string());

    if ephemeral || std::env::var("SYNKRO_EPHEMERAL").is_ok() {
        return Ok(DeviceConfig {
            device_id: Uuid::new_v4(),
            name,
            data_dir,
        });
    }

    let identity_file = data_dir.join("device_id");
    let device_id = if identity_file.exists() {
        let content = fs::read_to_string(&identity_file)?;
        Uuid::parse_str(content.trim()).unwrap_or_else(|_| {
            let id = Uuid::new_v4();
            let _ = fs::write(&identity_file, id.to_string());
            id
        })
    } else {
        let id = Uuid::new_v4();
        let _ = fs::write(&identity_file, id.to_string());
        id
    };

    Ok(DeviceConfig {
        device_id,
        name,
        data_dir,
    })
}

pub fn generated_room_code(id: Uuid) -> String {
    let compact = id.simple().to_string();
    compact[..6].to_ascii_uppercase()
}
