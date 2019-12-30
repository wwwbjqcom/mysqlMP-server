/*
@author: xiao cai niao
@datetime: 2019/12/30
*/


use actix_web::web;
use crate::storage::rocks::{DbInfo, PrefixTypeCode, CfNameTypeCode, KeyValue};
use std::{thread, time};
use crate::ha::nodes_manager::DifferenceSql;
use std::error::Error;

///
impl DifferenceSql{
    ///
    /// 过期删除超过7天的数据
    fn expired(&self, db: &web::Data<DbInfo>) -> Result<(), Box<dyn Error>>{
        if (crate::timestamp() - self.time) >= 604800000{
            self.delete(db)?;
        }
        Ok(())
    }

    fn delete(&self, db: &web::Data<DbInfo>) -> Result<(), Box<dyn Error>>{
        let key = format!("{}:{}:{}_{}", PrefixTypeCode::RollBackSql.prefix(),&self.cluster, &self.host, &self.time);
        db.delete(&key, &CfNameTypeCode::SystemData.get())?;
        Ok(())
    }
}

fn expired_rollback(db: &web::Data<DbInfo>) {
    //差异sql部分
    let result = db.get_rollback_sql(&String::from(""));
    match result {
        Ok(v) => {
            for row in &v{
                if let Err(e) = row.value.expired(db){
                    info!("{:?}", e.to_string());
                };
            }
        }
        Err(e) => {
            info!("{:?}", e.to_string());
        }
    }
}

pub fn manager(db: web::Data<DbInfo>) {
    let mut start_time = crate::timestamp();
    loop {
        if crate::timestamp() - start_time >= 3600000 {
            //每1小时清理一次数据
            expired_rollback(&db);
            start_time = crate::timestamp();
        }
        thread::sleep(time::Duration::from_secs(1));
    }
}