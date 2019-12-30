/*
@author: xiao cai niao
@datetime: 2019/12/27
*/
use serde::{Serialize, Deserialize};
use actix_web::HttpResponse;
use std::error::Error;

#[derive(Serialize, Deserialize, Debug)]
pub struct ResponseState {
    pub status: u8,         //状态值， 0代表错误， 1代表正常， 2代表没有session
    pub err: String
}

impl ResponseState {
    pub fn ok() -> HttpResponse {
        HttpResponse::Ok()
            .json(ResponseState {
                status: 1,
                err: "OK".parse().unwrap()
            })
    }

    pub fn error(err: String) -> HttpResponse {
        //return Err(err.into())
        HttpResponse::Ok()
            .json(ResponseState {
                status: 0,
                err
            })
    }

    pub fn no_session() -> HttpResponse {
        let err = "no seesion, please longin".to_string();
        HttpResponse::Ok()
            .json(ResponseState {
                status: 2,
                err
            })
    }
}

pub fn response_state(a: Result<(), Box<dyn Error>>) -> HttpResponse {
    match a {
        Ok(()) => {
            return ResponseState::ok();
        },
        Err(e) => {
            return ResponseState::error(e.to_string());
        }
    }
}

#[derive(Serialize)]
pub struct ResponseValue<F: Serialize>{
    status: u8,
    value: F
}

pub fn response_value<F: Serialize>(value: &F) -> HttpResponse {
    HttpResponse::Ok()
        .json(ResponseValue{status:3, value})
}