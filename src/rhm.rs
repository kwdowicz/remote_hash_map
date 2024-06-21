use crate::storage::Storage;
use std::collections::HashMap;
use tokio::io::Result;
use async_trait::async_trait;

#[async_trait]
pub trait StorageTrait: Send + Sync {
    async fn new(id: &str) -> Result<Self> where Self: Sized;
    async fn load(&self) -> Result<String>;
    async fn save(&self, content: &str) -> Result<()>;
}

#[derive(Debug)]
pub enum RhmResult {
    NewInsertOk,
    PreviousValue(String),
    NoKey,
    Value(String),
}

impl RhmResult {
    pub fn value(&self) -> String {
        match self {
            RhmResult::NewInsertOk => "Ok".to_string(),
            RhmResult::PreviousValue(value) => value.to_string(),
            RhmResult::NoKey => "NoKey".to_string(),
            RhmResult::Value(value) => value.to_string(),
        }
    }
}

impl PartialEq<&str> for RhmResult {
    fn eq(&self, other: &&str) -> bool {
        match self {
            RhmResult::Value(s) => s == *other,
            RhmResult::PreviousValue(s) => s == *other,
            RhmResult::NewInsertOk => "Ok" == *other,
            RhmResult::NoKey => "NoKey" == *other,
        }
    }
}

#[derive(Debug)]
pub struct Rhm {
    pub items: HashMap<String, String>,
    storage: Storage,
}

impl Rhm {
    pub async fn new(id: &str) -> Result<Self> {
        let mut rhm = Rhm {
            items: HashMap::new(),
            storage: Storage::new(id).await?,
        };
        rhm.load().await?;
        Ok(rhm)
    }

    async fn load(&mut self) -> Result<()> {
        let contents = self.storage.load().await?;
        for line in contents.lines() {
            let mut parts = line.splitn(3, '|');
            match (parts.next(), parts.next(), parts.next()) {
                (Some("SET"), Some(key), Some(value)) => {
                    self.items.insert(key.to_string(), value.to_string());
                }
                _ => {}
            }
        }
        Ok(())
    }

    pub async fn set(&mut self, key: &str, value: &str) -> Result<RhmResult> {
        let result = match self.items.insert(key.to_string(), value.to_string()) {
            Some(old_value) => RhmResult::PreviousValue(old_value),
            None => RhmResult::NewInsertOk,
        };
        self.storage.save(&format!("SET|{}|{}\r\n", key, value)).await?;
        Ok(result)
    }

    pub async fn get(&self, key: &str) -> RhmResult {
        self.items.get(key).map_or(RhmResult::NoKey, |v| RhmResult::Value(v.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_new_rhm() {
        let id = "test_1";
        let rhm = Rhm::new(id).await.unwrap();
        assert!(rhm.items.is_empty());
    }

    #[tokio::test]
    async fn test_set_get() {
        let id = "test_2";
        let mut rhm = Rhm::new(id).await.unwrap();
        rhm.set("key1", "value1").await.unwrap();
        assert_eq!(rhm.get("key1").await, "value1");
    }

}