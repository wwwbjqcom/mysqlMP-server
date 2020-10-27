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
    fn save(&self, db: &web::Data<DbInfo>, host: &String, last_value: &mut MysqlMonitorStatus) -> Result<(), Box<dyn Error>>{
        let prefix = PrefixTypeCode::NodeMonitorData;
        let key = format!("{}_{}", host, &self.time);
        let ms = self.calculation(last_value);
        //info!("{:?}", &ms);
        db.prefix_put(&prefix, &key, &ms)?;
//        let wb = self.batch(db, host)?;
//        db.db.write(wb)?;
        Ok(())
    }

    ///
    /// 计算两次监控之间数据差， 并计算平均到秒
    fn calculation(&self, last_value: &mut MysqlMonitorStatus) -> MysqlMonitorStatus{
        let time_dif = ((self.time - last_value.time) / 1000) as usize;
        MysqlMonitorStatus{
            com_insert: (self.com_insert - last_value.com_insert) / time_dif,
            com_update: (self.com_update - last_value.com_update) / time_dif,
            com_delete: (self.com_delete - last_value.com_delete) / time_dif,
            com_select: (self.com_select - last_value.com_select) / time_dif,
            questions: (self.questions - last_value.questions) / time_dif,
            innodb_row_lock_current_waits: self.innodb_row_lock_current_waits,
            innodb_row_lock_time: (self.innodb_row_lock_time - last_value.innodb_row_lock_time) / time_dif,
            created_tmp_disk_tables: (self.created_tmp_disk_tables - last_value.created_tmp_disk_tables) / time_dif,
            created_tmp_tables: (self.created_tmp_tables - last_value.created_tmp_tables) / time_dif,
            innodb_buffer_pool_reads: (self.innodb_buffer_pool_reads - last_value.innodb_buffer_pool_reads) /time_dif,
            innodb_buffer_pool_read_requests: (self.innodb_buffer_pool_read_requests - last_value.innodb_buffer_pool_read_requests) / time_dif,
            handler_read_first: (self.handler_read_first - last_value.handler_read_first) / time_dif,
            handler_read_key: (self.handler_read_key - last_value.handler_read_key) / time_dif,
            handler_read_next: (self.handler_read_next - last_value.handler_read_next) / time_dif,
            handler_read_prev: (self.handler_read_prev - last_value.handler_read_prev) / time_dif,
            handler_read_rnd: (self.handler_read_rnd - last_value.handler_read_rnd) / time_dif,
            handler_read_rnd_next: (self.handler_read_rnd_next - last_value.handler_read_rnd_next) / time_dif,
            innodb_os_log_pending_fsyncs: self.innodb_os_log_pending_fsyncs,
            innodb_os_log_pending_writes: self.innodb_os_log_pending_writes,
            innodb_log_waits: self.innodb_log_waits,
            threads_connected: self.threads_connected,
            threads_running: self.threads_running,
            bytes_sent: (self.bytes_sent - last_value.bytes_sent) / time_dif,
            bytes_received: (self.bytes_received - last_value.bytes_received) / time_dif,
            slow_queries: (self.slow_queries - last_value.slow_queries) / time_dif,
            time: self.time
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
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

    pub fn delete(&self, db: &web::Data<DbInfo>) -> Result<(), Box<dyn Error>>{
        let key = format!("{}:{}", &PrefixTypeCode::NodeMonitorSeting.prefix(), &self.host);
        db.delete(&key, &CfNameTypeCode::SystemData.get())?;
        Ok(())
    }
}


impl MysqlMonitorStatus{
    fn new() -> MysqlMonitorStatus{
        MysqlMonitorStatus{
            com_insert: 0,
            com_update: 0,
            com_delete: 0,
            com_select: 0,
            questions: 0,
            innodb_row_lock_current_waits: 0,
            innodb_row_lock_time: 0,
            created_tmp_disk_tables: 0,
            created_tmp_tables: 0,
            innodb_buffer_pool_reads: 0,
            innodb_buffer_pool_read_requests: 0,
            handler_read_first: 0,
            handler_read_key: 0,
            handler_read_next: 0,
            handler_read_prev: 0,
            handler_read_rnd: 0,
            handler_read_rnd_next: 0,
            innodb_os_log_pending_fsyncs: 0,
            innodb_os_log_pending_writes: 0,
            innodb_log_waits: 0,
            threads_connected: 0,
            threads_running: 0,
            bytes_sent: 0,
            bytes_received: 0,
            slow_queries: 0,
            time: 0
        }
    }
}

struct MonitorNodeSetInfo{
    setting: MonitorSetting,            //配置
    last_monitor_value: MysqlMonitorStatus  //上一次检查数据，用于计算差值
}
impl MonitorNodeSetInfo{
    fn new(ms: &MonitorSetting) -> MonitorNodeSetInfo{
        MonitorNodeSetInfo{
            setting: ms.clone(),
            last_monitor_value: MysqlMonitorStatus::new()
        }
    }

    fn update_value(&mut self, ms: &MysqlMonitorStatus) {
        self.last_monitor_value = ms.clone();
    }

    fn monitor_state(&mut self, db:&web::Data<DbInfo>) -> Result<(), Box<dyn Error>>{
        let monitor_data = MyProtocol::get_monitor(&MyProtocol::GetMonitor, &self.setting.host)?;
        //info!("{:?}", &monitor_data);
        if self.last_monitor_value.time != 0 {
            monitor_data.save(db, &self.setting.host, &mut self.last_monitor_value)?;
        }
        self.last_monitor_value = monitor_data;
        Ok(())
    }
}

fn monitor(db: &web::Data<DbInfo>, setting: &mut Vec<MonitorNodeSetInfo>) {
    for rw in setting{
        if !rw.setting.monitor{continue;}
        if let Err(e) = rw.monitor_state(db){
            if rw.last_monitor_value.time != 0{
                rw.last_monitor_value = MysqlMonitorStatus::new();
            }
            info!("get monitor data failed({}):{}", &rw.setting.host, e.to_string());
        }
    }
}
///
/// 删除过期监控数据， 默认最多保留30天的数据
fn expired_monitor_data(db: &web::Data<DbInfo>) {
    let monitor_set = db.get_monitor_setting();
    match monitor_set {
        Ok(set) => {
            if let Err(e) = db.expired_monitor_data(&set){
                info!("clear outdated monitoring data faild: {}", e.to_string());
            }
        }
        Err(e) => {
            info!("get monitor setting error: {:?}", e.to_string());
        }
    }
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
    info!("database purge thread start success");
    let mut sche_start_time = crate::timestamp();
    let mut loop_start_time = crate::timestamp();
    let mut monitor_set = db.get_monitor_setting().unwrap();
    let mut ms = vec![];
    for rw in monitor_set{
        ms.push(MonitorNodeSetInfo::new(&rw.value));
    }
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
            monitor_set = db.get_monitor_setting().unwrap();
            ms = init_monitor_set(&monitor_set, &ms);
            loop_start_time = crate::timestamp();
        }

        monitor(&db, &mut ms);

        thread::sleep(time::Duration::from_secs(10));
    }
}

///
/// 初始化监控配置
fn init_monitor_set(ms: &Vec<RowValue<MonitorSetting>>, mif: &Vec<MonitorNodeSetInfo>) -> Vec<MonitorNodeSetInfo>{
    let mut mm = vec![];
    for rw in ms{
        let mut mi = MonitorNodeSetInfo::new(&rw.value);
        for ii in mif{
            if ii.setting.host == rw.value.host{
                mi.update_value(&ii.last_monitor_value);
            }
        }
        mm.push(mi);
    }
    mm
}