/*
@author: xiao cai niao
@datetime: 2019/11/5
*/
use actix_web::{web, HttpResponse};
use serde::{Deserialize, Serialize};
use crate::storage;
use crate::storage::rocks::DbInfo;
use crate::ha::procotol::{MysqlState, HostInfoValue, AllNodeInfo};


#[derive(Serialize, Deserialize, Debug)]
pub struct HostInfo {
    pub host: String,   //127.0.0.1:3306
    pub rtype: String,  //db、route
    pub dbport: String, //default 3306
    pub cluster_name: String   //集群名称
}

#[derive(Serialize, Deserialize, Debug)]
pub struct State {
    pub state: u8,
    pub value: String
}


/// extract `import host info` using serde
pub fn import_mysql_info(data: web::Data<DbInfo>, info: web::Form<HostInfo>) -> HttpResponse {
    let state = storage::opdb::insert_mysql_host_info(data, &info);
    HttpResponse::from(match state {
        Ok(()) => {
            HttpResponse::Ok()
                .json(State{
                    state: 1, value: "OK".parse().unwrap()
                })
        }
        Err(e) => {
            HttpResponse::Ok()
                .json(State{
                    state: 0, value: e
                })
        }
    })
}

/// extract `export mysql host info` using serde
pub fn get_all_mysql_info(data: web::Data<DbInfo>) -> HttpResponse {
    let cf_name = String::from("Ha_nodes_info");
    let result = data.iterator(&cf_name,&String::from(""));

    let mut rows = AllNodeInfo::new();
    match result {
        Ok(v) => {
            for row in v {
                let role = get_nodes_role(&data, &row.key);
                let value: HostInfoValue = serde_json::from_str(&row.value).unwrap();
                let v = crate::ha::procotol::HostInfoValueGetAllState::new(&value, role);
                if &value.rtype == &String::from("route") {
                    rows.cluster_op(String::from("route"), v);
                }else {
                    rows.cluster_op(value.cluster_name, v);
                }
                //rows.push(serde_json::json!(v));
            }
            HttpResponse::Ok()
                .json(rows.rows)
        }
        Err(_e) => {
            HttpResponse::Ok()
                .json(vec![AllNodeInfo::new()])
        }
    }
}

fn get_nodes_role(data: &web::Data<DbInfo>, key: &String) -> String {
    let cf_name = String::from("Nodes_state");
    let v = data.get(key, &cf_name);
    match v {
        Ok(value) => {
            if value.value.len() > 0 {
                let re: MysqlState = serde_json::from_str(&value.value).unwrap();
                return re.role;
            }
        }
        Err(e) => {
            info!("{}",e.to_string());
        }
    }
    return String::from("");
}





