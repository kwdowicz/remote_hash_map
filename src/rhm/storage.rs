use crate::common::*;

#[derive(Debug)]
pub struct Storage {
    file: File,
}

impl Storage {
    pub async fn new(id: &str) -> tokio::io::Result<Self> {
        let data_file = data_file(id);
        if Self::not_exists(&data_file).await? {
            Self::create(&data_file).await?
        }

        Ok(Self { file: Self::open(&data_file).await? })
    }

    async fn not_exists(data_file: &str) -> std::io::Result<bool> {
        match metadata(data_file).await {
            Ok(_) => Ok(false),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(true),
            Err(e) => Err(e),
        }
    }

    async fn create(data_file: &str) -> tokio::io::Result<()> {
        File::create(data_file).await?;
        Ok(())
    }

    pub async fn open(data_file: &str) -> tokio::io::Result<File> {
        let file = OpenOptions::new().append(true).read(true).open(data_file).await?;
        Ok(file)
    }

    pub async fn save(&mut self, data: &str) -> tokio::io::Result<()> {
        self.file.write_all(data.as_bytes()).await?;
        Ok(())
    }

    pub async fn load(&mut self) -> tokio::io::Result<String> {
        let mut contents = String::new();
        self.file.read_to_string(&mut contents).await?;
        Ok(contents)
    }
}
