/*
@author: xiao cai niao
@datetime: 2019/12/30
*/


use actix_web::web;
use rocksdb::{WriteBatch};
use crate::storage::rocks::{DbInfo, PrefixTypeCode, CfNameTypeCode, RowValue};
use std::{thread, time};
use crate::ha::nodes_manager::DifferenceSql;
use std::error::Error;
use crate::storage::opdb::NodeClusterList;
use crate::ha::route_manager::RouteInfo;
use serde::{Serialize, Deserialize};
use crate::ha::procotol::{MyProtocol, MysqlMonitorStatus};

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
///
impl MysqlMonitorStatus{
    fn save(&self, db: &web::Data<DbInfo>, host: &String) -> Result<(), Box<dyn Error>>{
        let prefix = PrefixTypeCode::NodeMonitorData;
        let key = format!("{}_{}", host, &self.time);
        db.prefix_put(&prefix, &key, &self)?;
//        let wb = self.batch(db, host)?;
//        db.db.write(wb)?;
        Ok(())
    }

    fn delete(&self, db: &web::Data<DbInfo>, host: &String) -> Result<(), Box<dyn Error>>{
        let prefix = PrefixTypeCode::NodeMonitorData.prefix();
        let key = format!("{}:{}_{}", &prefix, host, &self.time);
        db.delete(&key, &CfNameTypeCode::SystemData.get())?;
        Ok(())
    }

    fn batch(&self, db:&web::Data<DbInfo>, host: &String) -> Result<WriteBatch, Box<dyn Error>>{
        let mut wb = WriteBatch::default();
        match db.db.cf_handle(&CfNameTypeCode::SystemData.get()){
            Some(cf) => {
                wb.put_cf(cf, self.get_key(host, String::from("com_insert")), &self.com_insert.to_string())?;
                wb.put_cf(cf,self.get_key(host, String::from("com_insert")), &self.com_insert.to_string())?;
                wb.put_cf(cf,self.get_key(host, String::from("com_update")), &self.com_update.to_string())?;
                wb.put_cf(cf,self.get_key(host, String::from("com_delete")), &self.com_delete.to_string())?;
                wb.put_cf(cf,self.get_key(host, String::from("com_select")), &self.com_select.to_string())?;
                wb.put_cf(cf,self.get_key(host, String::from("questions")), &self.questions.to_string())?;
                wb.put_cf(cf,self.get_key(host, String::from("innodb_row_lock_current_waits")), &self.innodb_row_lock_current_waits.to_string())?;
                wb.put_cf(cf,self.get_key(host, String::from("innodb_row_lock_time")), &self.innodb_row_lock_time.to_string())?;
                wb.put_cf(cf,self.get_key(host, String::from("created_tmp_disk_tables")), &self.created_tmp_disk_tables.to_string())?;
                wb.put_cf(cf,self.get_key(host, String::from("created_tmp_tables")), &self.created_tmp_tables.to_string())?;
                wb.put_cf(cf,self.get_key(host, String::from("innodb_buffer_pool_reads")), &self.innodb_buffer_pool_reads.to_string())?;
                wb.put_cf(cf,self.get_key(host, String::from("innodb_buffer_pool_read_requests")), &self.innodb_buffer_pool_read_requests.to_string())?;
                wb.put_cf(cf,self.get_key(host, String::from("handler_read_first")), &self.handler_read_first.to_string())?;
                wb.put_cf(cf,self.get_key(host, String::from("handler_read_key")), &self.handler_read_key.to_string())?;
                wb.put_cf(cf,self.get_key(host, String::from("handler_read_next")), &self.handler_read_next.to_string())?;
                wb.put_cf(cf,self.get_key(host, String::from("handler_read_prev")), &self.handler_read_prev.to_string())?;
                wb.put_cf(cf,self.get_key(host, String::from("handler_read_rnd")), &self.handler_read_rnd.to_string())?;
                wb.put_cf(cf,self.get_key(host, String::from("handler_read_rnd_next")), &self.handler_read_rnd_next.to_string())?;
                wb.put_cf(cf,self.get_key(host, String::from("innodb_os_log_pending_fsyncs")), &self.innodb_os_log_pending_fsyncs.to_string())?;
                wb.put_cf(cf,self.get_key(host, String::from("innodb_os_log_pending_writes")), &self.innodb_os_log_pending_writes.to_string())?;
                wb.put_cf(cf,self.get_key(host, String::from("innodb_log_waits")), &self.innodb_log_waits.to_string())?;
                wb.put_cf(cf,self.get_key(host, String::from("threads_connected")), &self.threads_connected.to_string())?;
                wb.put_cf(cf,self.get_key(host, String::from("threads_running")), &self.threads_running.to_string())?;
                wb.put_cf(cf,self.get_key(host, String::from("bytes_sent")), &self.bytes_sent.to_string())?;
                wb.put_cf(cf,self.get_key(host, String::from("bytes_received")), &self.bytes_received.to_string())?;
            }
            None =>{}
        }
        Ok(wb)
    }

    fn get_key(&self, host: &String, key: String) -> String{
        let a = format!("{}_{}_{}_{}", &PrefixTypeCode::NodeMonitorData.prefix(),host.clone(), key, self.time.clone());
        return a;
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct MonitorSetting{
    pub host: String,
    pub monitor: bool,
    pub days: u8,   //保留天数, 默认7天
}
impl MonitorSetting{
    pub fn new(host: &String) -> MonitorSetting {
        MonitorSetting{
            host: host.clone(),
            monitor: false,
            days: 7
        }
    }

    pub fn save(&self, db: &web::Data<DbInfo>) -> Result<(), Box<dyn Error>>{
        db.prefix_put(&PrefixTypeCode::NodeMonitorSeting, &self.host, &self)?;
        Ok(())
    }

    fn get_monitor_state(&self, db: &web::Data<DbInfo>) -> Result<(), Box<dyn Error>>{
        if self.monitor{
            let monitor_data = MyProtocol::get_monitor(&MyProtocol::GetMonitor, &self.host)?;
            monitor_data.save(db, &self.host)?;
        }
        Ok(())
    }
}

fn monitor(db: &web::Data<DbInfo>, setting: &Vec<RowValue<MonitorSetting>>) {
    for rw in setting{
        if let Err(e) = rw.value.get_monitor_state(db){
            info!("get monitor data from {:?} failed: {:?}", rw.key,e.to_string());
        }
    }
}
///
/// 删除过期监控数据， 默认最多保留30天的数据
fn expired_monitor_data(db: &web::Data<DbInfo>) {
//    let monitor_set = db.get_monitor_setting();
//    match monitor_set {
//        Ok(set) => {
//            if let Err(e) = db.expired_monitor_data(&monitor_set){
//                info!("clear outdated monitoring data faild: {}", e.to_string());
//            }
//        }
//        Err(e) => {
//            info!("get monitor setting error: {:?}", e.to_string());
//        }
//    }
}

///
///
///
/// 清理数据
fn expired(db: web::Data<DbInfo>){
    expired_rollback(&db);
    expired_dirty_route_info(&db);
    expired_monitor_data(&db);
}

pub fn manager(db: web::Data<DbInfo>) {
    let mut sche_start_time = crate::timestamp();
    let mut loop_start_time = crate::timestamp();
    let mut monitor_set = db.get_monitor_setting();
    loop {
        if crate::timestamp() - sche_start_time >= (3600000 * 24) {
            //每24小时清理一次数据
            let b = db.clone();
            thread::spawn(move ||{
                expired(b);
            });
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
        thread::sleep(time::Duration::from_secs(10));
    }
}