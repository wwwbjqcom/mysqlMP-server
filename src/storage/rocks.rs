/*
@author: xiao cai niao
@datetime: 2019/11/18
*/

use rocksdb::{DB, Options, DBCompactionStyle};
use std::error::Error;
use std::str::from_utf8;
use serde::{Deserialize, Serialize};
use crate::storage::opdb::{UserInfo, SlaveBehindSetting};
use crate::webroute::route::PostUserInfo;
use crate::ha::nodes_manager::DifferenceSql;
use crate::ha::route_manager::RouteInfo;
use crate::ha::sys_manager::MonitorSetting;


pub enum PrefixTypeCode {
    RouteInfo,              //保存集群路由信息的前缀
    RollBackSql,            //保存回滚sql信息的前缀
    UserInfo,               //用户信息
    SlaveDelaySeting,       //每个集群slave最大延迟时间配置， 用于路由剔除
    NodeMonitorSeting,      //每个节点打开监控的配置
    NodeMonitorData,        //每个节点的监控数据
}

impl PrefixTypeCode {
    pub fn prefix(&self) -> String {
        let prefix = "crcp".to_string();
        match self {
            PrefixTypeCode::RouteInfo => {
                format!("{}{}",0x01, &prefix)
            }
            PrefixTypeCode::RollBackSql => {
                format!("{}{}",0x02, &prefix)
            }
            PrefixTypeCode::UserInfo => {
                format!("{}{}",0x03, &prefix)
            }
            PrefixTypeCode::SlaveDelaySeting => {
                format!("{}{}",0x04, &prefix)
            }
            PrefixTypeCode::NodeMonitorSeting => {
                format!("{}{}", 0x05, &prefix)
            }
            PrefixTypeCode::NodeMonitorData => {
                format!("{}{}", 0x06, &prefix)
            }
        }
    }
}

pub enum CfNameTypeCode {
    HaNodesInfo,            //保存节点基础数据
    RollbackSqlInfo,        //保存宕机切换产生的回滚数据
    HaChangeLog,            //宕机切换日志
    NodesState,             //每个节点的状态数据
    SystemData,             //系统数据
    CheckState,             //存储宕机状态的节点信息
}

impl CfNameTypeCode {
    pub fn get(&self) -> String {
        match self{
            CfNameTypeCode::HaNodesInfo => String::from("Ha_nodes_info"),
            CfNameTypeCode::RollbackSqlInfo => String::from("Rollback_sql_info"),
            CfNameTypeCode::HaChangeLog => String::from("Ha_change_log"),
            CfNameTypeCode::SystemData => String::from("System_data"),
            CfNameTypeCode::NodesState => String::from("Nodes_state"),
            CfNameTypeCode::CheckState => String::from("Check_state")
        }
    }
}

#[derive(Debug)]
pub struct RowValue<T: Serialize>{
    pub key: String,
    pub value: T
}

#[derive(Deserialize, Debug, Serialize, Eq, Ord, PartialEq, PartialOrd)]
pub struct KeyValue{
    pub key: String,
    pub value: String
}

impl KeyValue{
    pub fn new(key: &String, value: &String) -> KeyValue {
        KeyValue{key: key.parse().unwrap(), value: value.parse().unwrap() }
    }
}

pub struct DbInfo{
    pub db: DB,
}
impl DbInfo {
    pub fn new() -> DbInfo {
        let cf_names: Vec<String> = vec![String::from("Ha_nodes_info"),
                                         String::from("Rollback_sql_info"),
                                         String::from("Ha_change_log"),
                                         String::from("System_data"),
                                         String::from("Nodes_state"),
                                         String::from("Check_state")];
        let db_state = init_db(&cf_names);
        match db_state {
            Ok(db) => {
                DbInfo{db}
            }
            Err(e) => {
                info!("{:?}",e.to_string());
                std::process::exit(1)
            }
        }
    }

    pub fn put(&self, kv: &KeyValue, cf_name: &String) -> Result<(), Box<dyn Error>> {
        self.check_cf(cf_name)?;
        match self.db.cf_handle(cf_name) {
            Some(cf) => {
                self.db.put_cf(cf, &kv.key, &kv.value)?;
            }
            None => {}
        }

        Ok(())
    }

    pub fn get(&self, key: &String, cf_name: &String) -> Result<KeyValue, Box<dyn Error>> {
        self.check_cf(cf_name)?;
        let mut kv = KeyValue::new(key, &String::from(""));
        match self.db.cf_handle(cf_name){
            Some(cf) => {
                let value = self.db.get_cf(cf, key)?;
                match value {
                    Some(v) => {
                        let value = from_utf8(&v).unwrap();
                        kv.value = value.parse().unwrap();
                    }
                    None => {}
                }
            }
            None => {}
        }

        return Ok(kv);
    }

    pub fn delete(&self, key: &String, cf_name: &String) -> Result<(), Box<dyn Error>> {
        self.check_cf(cf_name)?;
        match self.db.cf_handle(cf_name){
            Some(cf) => {
                self.db.delete_cf(cf, key)?;
            }
            None => {}
        }
        Ok(())
    }

    pub fn iterator(&self, cf_name: &String, seek_to: &String) -> Result<Vec<KeyValue>, Box<dyn Error>> {
        self.check_cf(cf_name)?;
        if let Some(cf) = self.db.cf_handle(cf_name){
            let mut iter = self.db.raw_iterator_cf(cf)?;
            if seek_to.len() > 0 {
                iter.seek(seek_to);
            }else {
                iter.seek_to_first();
            }
            let mut values: Vec<KeyValue> = vec![];
            while iter.valid() {
                let mut key: String = String::from("");
                let mut value: String = String::from("");
                if let Some(v) = iter.key() {
                    key = from_utf8(&v.to_vec())?.parse()?;
                }

                if let Some(v) = iter.value() {
                    value = from_utf8(&v.to_vec())?.parse()?;
                }

                let kv = KeyValue{key, value};
                values.push(kv);
                iter.next();
            }
            return Ok(values);
        }
        let a = format!("no cloumnfamily {}", cf_name);
        return  Err(a.into())
    }

    pub fn prefix_iterator(&self, prefix: &String, cf_name: &String) -> Result<Vec<KeyValue>, Box<dyn Error>> {
        self.check_cf(cf_name)?;
        if let Some(cf) = self.db.cf_handle(cf_name) {
            let iter = self.db.prefix_iterator_cf(cf,prefix)?;
            let mut values: Vec<KeyValue> = vec![];
            for (k, v) in iter {
                let key: String = from_utf8(&k.to_vec())?.parse()?;
                let value: String = from_utf8(&v.to_vec())?.parse()?;
                let kv = KeyValue{key, value};
                values.push(kv);
            }
            return Ok(values);
        }
        let a = format!("no cloumnfamily {}", cf_name);
        return  Box::new(Err(a)).unwrap();
    }

    pub fn prefix_put<T: Serialize>(&self, prefix_type: &PrefixTypeCode, key: &String, value: &T) -> Result<(), Box<dyn Error>> {
        let key = format!("{}:{}", prefix_type.prefix(), key);
        let value = serde_json::to_string(value)?;
        let kv = KeyValue{ key, value};
        self.put(&kv, &CfNameTypeCode::SystemData.get())?;
        Ok(())
    }

    pub fn prefix_get(&self, prefix_type: &PrefixTypeCode, key: &String) -> Result<KeyValue, Box<dyn Error>> {
        let key = format!("{}:{}", prefix_type.prefix(), key);
        let v = self.get(&key, &CfNameTypeCode::SystemData.get())?;
        Ok(KeyValue{
            key,
            value: v.value
        })
    }

    ///
    /// 检查列簇是否已存在
    ///
    pub fn check_cf(&self, cf_name: &String) -> Result<(), Box<dyn Error>> {
        let cf = self.db.cf_handle(cf_name);
        match cf {
            Some(_v) => {return Ok(());},
            None => {}
        };
        let a = format!("no cloumnfamily {}", cf_name);
        return  Box::new(Err(a)).unwrap();

    }

    pub fn init_admin_user(&self) -> Result<(), Box<dyn Error>> {
        let user_name = "admin".to_string();
        let password = "admin".to_string();
        let userinfo = UserInfo::new(&PostUserInfo{ user_name, password });
        let result = self.prefix_get(&PrefixTypeCode::UserInfo, &userinfo.user_name)?;
        if result.value.len() > 0 {
            return Ok(())
        }
        self.prefix_put(&PrefixTypeCode::UserInfo, &userinfo.user_name, &userinfo)?;
        Ok(())
    }

    pub fn get_rollback_sql(&self, prefix: &String) -> Result<Vec<RowValue<DifferenceSql>>, Box<dyn Error>>{
        let prefix = format!("{}:{}", PrefixTypeCode::RollBackSql.prefix(), prefix);
        let mut rw = vec![];
        let result = self.prefix_iterator(&prefix, &CfNameTypeCode::SystemData.get())?;
        for row in &result{
            if !row.key.starts_with(&prefix){continue;}
            let value: DifferenceSql = serde_json::from_str(&row.value)?;
            let r = RowValue{ key: row.key.clone(), value };
            rw.push(r);
        }
        Ok(rw)
    }

    pub fn get_route_all(&self) -> Result<Vec<RowValue<RouteInfo>>, Box<dyn Error>>{
        let prefix = format!("{}", PrefixTypeCode::RouteInfo.prefix());
        let mut rw = vec![];
        let result = self.prefix_iterator(&prefix, &CfNameTypeCode::SystemData.get())?;
        for row in &result{
            if !row.key.starts_with(&prefix){continue;}
            let value: RouteInfo = serde_json::from_str(&row.value)?;
            let r = RowValue{ key: row.key.clone(), value };
            rw.push(r);
        }
        Ok(rw)
    }


    ///
    /// 获取集群slave behind延迟配置， 如果未配置默认100
    pub fn get_hehind_setting(&self, cluster_name: &String) -> Result<SlaveBehindSetting, Box<dyn Error>>{
        let result = self.prefix_get(&PrefixTypeCode::SlaveDelaySeting, cluster_name)?;
        if result.value.len() > 0{
            let v: SlaveBehindSetting = serde_json::from_str(&result.value)?;
            return Ok(v)
        }
        return Ok(SlaveBehindSetting::new(cluster_name))
    }

    ///
    /// 获取所有节点监控开关配置
    pub fn get_monitor_setting(&self) -> Result<Vec<RowValue<MonitorSetting>>, Box<dyn Error>>{
        let prefix = PrefixTypeCode::NodeMonitorSeting.prefix();
        let mut rw = vec![];
        let result = self.prefix_iterator(&prefix, &CfNameTypeCode::SystemData.get())?;
        for row in result{
            if !row.key.starts_with(&prefix){continue;}
            if row.value.len() == 0 {continue;}
            let value: MonitorSetting = serde_json::from_str(&row.value)?;
            let r = RowValue{key: value.host.clone() , value};
            rw.push(r);
        }
        Ok(rw)
    }

    ///
    /// 删除过期监控数据
    pub fn expired_monitor_data(&self, monitor_set: &Vec<RowValue<MonitorSetting>>) -> Result<(), Box<dyn Error>>{
        let one_day_ms = (60 * 1000 * 60 * 24) as i64; // 一天多少毫秒
        let cur_time = crate::timestamp();
        let cf_name = CfNameTypeCode::SystemData.get();
        self.check_cf(&cf_name)?;
        if let Some(cf) = self.db.cf_handle(&cf_name){
            let mut iter = self.db.raw_iterator_cf(cf)?;
            iter.seek_to_first();
            while iter.valid() {
                if let Some(v) = iter.key() {
                    let key: String = from_utf8(&v.to_vec())?.parse()?;
                    if key.starts_with(PrefixTypeCode::NodeMonitorData.prefix()){
                        info!("{:?}", &key);
                        for mset in monitor_set{
                            if key.contains(mset.value.host.as_str()){
                                let key_info = key.split("_");
                                let key_info = key_info.collect::<Vec<&str>>();
                                let time = key_info[1].to_string().parse::<i64>()?;
                                if &(cur_time - time) > &(one_day_ms * mset.value.days as i64) {
                                    self.delete(&key, &cf_name)?;
                                }
                            }
                        }
                    }
                }
                iter.next();
            }
            return Ok(());
        }
        let a = format!("no cloumnfamily {}", cf_name);
        return  Err(a.into())
    }
}

fn init_db(cf_names: &Vec<String>) -> Result<DB, Box<dyn Error>> {
    let cf_info = DB::list_cf(&Options::default(),"rocksdb");
    match cf_info {
        Ok(c) =>{
            let opts = set_opts();
            let mut db = DB::open_cf(&opts,"rocksdb", &c)?;
            check_cf_exist(cf_names, &c, &mut db);
            return Ok(db);
        }
        Err(e) => {
            assert_eq!(e.to_string().find("No such file"), Some(10));
            info!("{:?}",e.to_string());
            info!("Create db file.....");
            let mut db = DB::open_default("rocksdb")?;
            info!("OK");
            let cl_list = vec![String::from("default")];
            check_cf_exist(cf_names, &cl_list, &mut db);
            return Ok(db);
        }
    }
}

fn check_cf_exist(cf_names: &Vec<String>, cf_list: &Vec<String>, db: &mut DB) {
    let opts = set_opts();
    'b: for cf in cf_names{
        'c: for cf_l in cf_list {
            if cf_l == cf{
                continue 'b;
            }
        }
        if let Err(e) = db.create_cf(cf, &opts){
            info!("{:?}",e.to_string());
        };
    }
}

fn set_opts() -> Options {
    let prefix_extractor = rocksdb::SliceTransform::create_fixed_prefix(5);
    let mut opts = Options::default();
    opts.set_prefix_extractor(prefix_extractor);
    opts.create_if_missing(true);
    opts.set_max_open_files(10000);
    opts.set_use_fsync(false);
    opts.set_bytes_per_sync(8388608);
    opts.optimize_for_point_lookup(1024);
    opts.set_table_cache_num_shard_bits(6);
    opts.set_max_write_buffer_number(32);
    opts.set_write_buffer_size(536870912);
    opts.set_target_file_size_base(1073741824);
    opts.set_min_write_buffer_number_to_merge(4);
    opts.set_level_zero_stop_writes_trigger(2000);
    opts.set_level_zero_slowdown_writes_trigger(0);
    opts.set_compaction_style(DBCompactionStyle::Universal);
    opts.set_max_background_compactions(4);
    opts.set_max_background_flushes(4);
    opts.set_disable_auto_compactions(true);
    return opts;
}

