/*
@author: xiao cai niao
@datetime: 2019/11/15
*/
pub mod route;

use actix_web::{http::header, HttpRequest, HttpResponse, Result};
use serde::{Deserialize, Serialize};
use actix_files::NamedFile;
use std::path::PathBuf;

#[derive(Serialize, Deserialize)]
pub struct MyObj {
    pub name: String,
}

pub fn index(req: HttpRequest) -> Result<HttpResponse> {
    let url = req.url_for("foo", &["index.html"])?; // <- generate url for "foo" resource
    Ok(HttpResponse::Found()
        .header(header::LOCATION, url.as_str())
        .finish())
}

pub fn index_static(req: HttpRequest) -> Result<NamedFile> {
    let path: PathBuf = req.match_info().query("filename").parse().unwrap();
    Ok(NamedFile::open(path)?)
}

pub fn index_file() -> Result<NamedFile>{
    Ok(NamedFile::open("index.html")?)
}

pub fn index_html() -> HttpResponse {
    HttpResponse::Ok()
        .json(MyObj{
            name: String::from("aa")
        })
}

