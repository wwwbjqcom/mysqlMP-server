/*
@author: xiao cai niao
@datetime: 2019/12/27
*/


use actix_web::web;
use crate::storage::rocks::{DbInfo, CfNameTypeCode, PrefixTypeCode};
use std::error::Error;
use crate::storage::opdb::{ClusterNodeInfo, HaChangeLog};
use crate::ha::nodes_manager::DifferenceSql;
use serde::Serialize;

#[derive(Serialize)]
pub struct ClusterMonitorStatus{
    pub manager_status: bool,
    pub cluster_name: String,
    pub failover_count: String
}
impl ClusterMonitorStatus{
    fn new(cluster_name: &String) -> ClusterMonitorStatus{
        ClusterMonitorStatus{
            manager_status: true,
            cluster_name: cluster_name.clone(),
            failover_count: "".to_string()
        }
    }
    fn update_failover_count(&mut self, cl_info: &ClusterNodeInfo){
        let mut failover = 0;
        for node in &cl_info.nodes_info{
            if !node.online {
                failover += 1;
            }
        }
        self.failover_count = format!("{}/{}", failover, &cl_info.total);
    }
}
#[derive(Serialize)]
pub struct ClusterInfo{
    pub topology: String,
    pub cluster_state: bool,
    pub difference_data: bool
}
impl ClusterInfo{
    fn new() -> ClusterInfo{
        ClusterInfo{
            topology: "master-slave".to_string(),
            cluster_state: false,
            difference_data: false
        }
    }

    fn init(&mut self, db: &web::Data<DbInfo>, cl_info: &ClusterNodeInfo) -> Result<(), Box<dyn Error>>{
        self.check_cluster_state(cl_info);
        self.check_difference_data(db, cl_info)?;
        Ok(())
    }

    fn check_cluster_state(&mut self, cl_info: &ClusterNodeInfo) {
        let mut master = 0;
        for node in &cl_info.nodes_info{
            if !node.online {continue;}
            if node.role == "master".to_string(){
                master += 1;
            }
        }
        if master == 1{
            self.cluster_state = true;
        }
    }

    fn check_difference_data(&mut self, db: &web::Data<DbInfo>, cl_info: &ClusterNodeInfo) -> Result<(), Box<dyn Error>>{
        let result = db.get_rollback_sql(&cl_info.cluster_name)?;
        for row in &result{
            if row.value.status == 0{
                if row.value.sqls.len() > 0{
                    self.difference_data = true;
                }
            }
        }
//        let prefix = format!("{}:{}",PrefixTypeCode::RollBackSql.prefix(), cl_info.cluster_name);
//        let sql_result = db.prefix_iterator(&prefix, &CfNameTypeCode::SystemData.get())?;
//        for info in &sql_result{
//            if !info.key.starts_with(&prefix){continue;}
//            let value: DifferenceSql = serde_json::from_str(&info.value).unwrap();
//            if &value.status == &0{
//                if value.sqls.len() > 0{
//                    self.difference_data = true;
//                }
//            }
//        }
        Ok(())
    }
}

#[derive(Serialize)]
pub struct ClusterSwitchInfo{
    pub switch_total: usize,
    pub last_switch_time: i64,
    pub last_switch_state: bool
}
impl ClusterSwitchInfo{
    fn new() -> ClusterSwitchInfo{
        ClusterSwitchInfo{
            switch_total: 0,
            last_switch_time: 0,
            last_switch_state: true
        }
    }
    fn init(&mut self, db: &web::Data<DbInfo>, cl_info: &ClusterNodeInfo) -> Result<(), Box<dyn Error>>{
        let result = db.prefix_iterator(&String::from(""), &CfNameTypeCode::HaChangeLog.get())?;
        for row in result{
            if row.value.len() == 0 {continue;}
            let value: HaChangeLog = serde_json::from_str(&row.value)?;
            if value.cluster_name == cl_info.cluster_name {
                self.switch_total += 1;
                let tmp_list = row.key.split("_");
                let tmp_list = tmp_list.collect::<Vec<&str>>();
                let tmp_time: i64 = tmp_list[1].to_string().parse()?;
                if tmp_time > self.last_switch_time {
                    self.last_switch_time = tmp_time;
                    self.last_switch_state = value.switch_status;
                }
            }
        }
        Ok(())
    }
}
#[derive(Serialize)]
pub struct ClusterMonitorInfo{
    pub monitor_status: ClusterMonitorStatus,
    pub cluster_info: ClusterInfo,
    pub switch_info: ClusterSwitchInfo
}
impl ClusterMonitorInfo{
    pub fn new(cluster_name: &String) -> ClusterMonitorInfo {
        ClusterMonitorInfo{
            monitor_status: ClusterMonitorStatus::new(cluster_name),
            cluster_info: ClusterInfo::new(),
            switch_info: ClusterSwitchInfo::new()
        }
    }

    pub fn init(&mut self, db: &web::Data<DbInfo>, cluster_name: &String) -> Result<(), Box<dyn Error>> {
        let mut cl_info = ClusterNodeInfo::new(cluster_name);
        cl_info.init(db)?;
        self.init_monitor_status(&cl_info);
        self.init_cluster_info(db, &cl_info)?;
        self.init_switch_log_info(db, &cl_info)?;
        Ok(())
    }

    fn init_monitor_status(&mut self, cl_info: &ClusterNodeInfo) {
        self.monitor_status.update_failover_count(cl_info);
    }

    fn init_cluster_info(&mut self, db: &web::Data<DbInfo>, cl_info: &ClusterNodeInfo) -> Result<(), Box<dyn Error>> {
        self.cluster_info.init(db, cl_info)?;
        Ok(())
    }

    fn init_switch_log_info(&mut self, db: &web::Data<DbInfo>, cl_info: &ClusterNodeInfo) -> Result<(), Box<dyn Error>>{
        self.switch_info.init(db, cl_info)?;
        Ok(())
    }
}