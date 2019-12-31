/*
@author: xiao cai niao
@datetime: 2019/12/30
*/


use actix_web::web;
use crate::storage::rocks::{DbInfo, PrefixTypeCode, CfNameTypeCode, RowValue};
use std::{thread, time};
use crate::ha::nodes_manager::DifferenceSql;
use std::error::Error;
use crate::storage::opdb::NodeClusterList;
use crate::ha::route_manager::RouteInfo;
use serde::{Serialize, Deserialize};

///
///
///
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
/// 删除超过7天的差异sql
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


///
///
///
///
///
impl RouteInfo{
    fn expired(&self, db: &web::Data<DbInfo>, cl_list: &NodeClusterList) -> Result<(), Box<dyn Error>>{
        for cluster_name in &cl_list.cluster_name_list{
            if cluster_name == &self.cluster_name{
                return Ok(());
            }
        }
        return self.delete(db);
    }

    fn delete(&self, db: &web::Data<DbInfo>) -> Result<(), Box<dyn Error>> {
        let key = format!("{}:{}", &PrefixTypeCode::RouteInfo.prefix(), &self.cluster_name);
        db.delete(&key, &CfNameTypeCode::SystemData.get())?;
        Ok(())
    }
}
/// 删除无效的路由信息
fn expired_dirty_route_info(db: &web::Data<DbInfo>) {
    let mut n = NodeClusterList::new();
    if let Err(e) = n.init(db){
        info!("{:?}", e.to_string());
        return;
    }

    let routes = db.get_route_all();
    match routes {
        Ok(r) => {
            'a: for route_info in &r{
                if let Err(e) = route_info.value.expired(db, &n){
                    info!("{:?}", e.to_string());
                }
            }
        }
        Err(e) => {
            info!("{:?}", e.to_string());
        }
    }

}

///
///
///
#[derive(Serialize, Deserialize, Debug)]
pub struct MonitorSetting{
    pub host: String,
    pub monitor: bool,
}
impl MonitorSetting{
    fn get_monitor_state(&self, db: &web::Data<DbInfo>) -> Result<(), Box<dyn Error>>{
        if self.monitor{

        }
        Ok(())
    }
}

fn monitor(db: &web::Data<DbInfo>, setting: &Vec<RowValue<MonitorSetting>>) {

}

pub fn manager(db: web::Data<DbInfo>) {
    let mut sche_start_time = crate::timestamp();
    let mut loop_start_time = crate::timestamp();
    let mut monitor_set = db.get_monitor_setting();
    loop {
        if crate::timestamp() - sche_start_time >= (3600000 * 24) {
            //每24小时清理一次数据
            expired_rollback(&db);
            expired_dirty_route_info(&db);
            sche_start_time = crate::timestamp();
        }

        if crate::timestamp() - loop_start_time >= 60000 {
            //每60秒重新获取一次配置信息
            monitor_set = db.get_monitor_setting();
            loop_start_time = crate::timestamp();
        }

        match &monitor_set {
            Ok(s) => {
                monitor(&db, s);
            }
            Err(e) => {
                info!("get monitor setting error: {:?}", e.to_string());
            }
        }


        thread::sleep(time::Duration::from_secs(1));
    }
}