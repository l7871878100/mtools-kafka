#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::PathBuf,
};

use eframe::egui::{self, CollapsingHeader, FontData, FontFamily, TextBuffer};
use egui_extras::{Column, TableBuilder};
use kafka::{
    consumer::{Consumer, FetchOffset},
    producer::AsBytes,
};
use kafka::client::{KafkaClient, PartitionOffset};
use kafka::consumer::GroupOffsetStorage;
use serde_derive::{Deserialize, Serialize};

static APP_NAME: &str = "Kafka Tool";
static KAFKA_GROUP_ID: &str = "mtools";

#[derive(Default)]
struct ToolApp {
    config: ToolConfig,
    temp_config: KafkaConfig,
    current_config: KafkaConfig,
    kafka_client: Option<KafkaClient>,
    panel_id: String,
    list_panel_id: Option<String>,
    current_topic: String,
    current_offset_type: String,
    current_messages: Vec<KafkaMessage>,
    partition_offsets: Vec<PartitionOffset>,
    value_filter: String,
}

#[derive(Default, Debug, Clone)]
struct KafkaMessage {
    offset: i64,
    key: String,
    value: String,
}

#[derive(Serialize, Deserialize, Default)]
struct ToolConfig {
    kafka_configs: Vec<KafkaConfig>,
}

impl ToolApp {
    pub fn new(cc: &eframe::CreationContext<'_>) -> Self {
        // 添加中文字体支持，因为egui默认不支持中文
        let mut fonts = eframe::egui::FontDefinitions::default();
        fonts.font_data.insert("my_font".to_owned(),
                               FontData::from_static(include_bytes!("../HarmonyOS_Sans_SC_Regular.ttf"))); // .ttf and .otf supported

        fonts.families.get_mut(&FontFamily::Proportional).unwrap()
            .insert(0, "my_font".to_owned());

        fonts.families.get_mut(&FontFamily::Monospace).unwrap()
            .push("my_font".to_owned());

        cc.egui_ctx.set_fonts(fonts);
        // 初始化
        let mut app = ToolApp {
            current_offset_type: "起始".to_owned(),
            ..ToolApp::default()
        };
        // 加载配置
        app.load_config();
        app
    }

    // 保存配置
    pub fn save_config(&mut self) {
        let mut file = OpenOptions::new()
            .read(false)
            .write(true)
            .create(true)
            .open(".config")
            .unwrap();
        file.write_all(serde_json::to_string(&self.config).unwrap().as_bytes())
            .unwrap();
    }

    // 启动加载配置
    pub fn load_config(&mut self) {
        let path = PathBuf::from(".config");
        if path.exists() {
            let mut file = File::open(path.clone()).unwrap();
            let mut buf = Vec::new();
            file.read_to_end(&mut buf).unwrap();

            let data: Result<ToolConfig, serde_json::Error> =
                serde_json::from_slice(buf.as_bytes());

            match data {
                Ok(data) => self.config = data,
                Err(e) => {
                    // 加载失败,删除旧配置
                    println!("{:?}", e);
                    fs::remove_file(&path).unwrap()
                }
            };
        }
    }
}

#[derive(Default, Serialize, Deserialize, Clone)]
struct KafkaConfig {
    id: String,
    group_name: String,
    name: String,
    host: String,
    topics: Vec<String>,
    group_ids: Vec<String>,
    message: Option<String>,
    poll_rows: usize,
}

impl eframe::App for ToolApp {
    fn update(&mut self, ctx: &egui::Context, frame: &mut eframe::Frame) {
        // 菜单栏
        egui::TopBottomPanel::top("main_top_panel").show(ctx, |ui| {
            ui.horizontal(|ui| {
                ui.menu_button("连接", |ui| {
                    if ui.button("新建").clicked() {
                        self.panel_id = "new".to_owned();
                    };
                });
                ui.menu_button("帮助", |ui| {
                    if ui.button("关于").clicked() {
                        println!("点击了关于按钮");
                    };
                });
            });
        });
        // 左侧栏
        egui::SidePanel::left("main_left_side").show(ctx, |ui| {
            let mut group_names = vec![];

            for kafka_config in &self.config.kafka_configs {
                let kafka_config_group_name = kafka_config.group_name.clone();
                if !group_names.contains(&kafka_config_group_name) {
                    group_names.push(kafka_config_group_name.clone());
                }
            }

            for kafka_group_name in group_names {
                CollapsingHeader::new(kafka_group_name.clone()).show(ui, |ui| {
                    for ele in &self.config.kafka_configs {
                        if kafka_group_name == ele.group_name {
                            CollapsingHeader::new(ele.name.clone()).show(ui, |ui| {
                                CollapsingHeader::new("主题").show(ui, |ui| {
                                    for topic in &ele.topics {
                                        if ui.button(topic.clone()).clicked() {
                                            self.panel_id = "list".to_owned();
                                            self.list_panel_id = Some("data".to_owned());
                                            self.current_config = ele.clone();
                                            self.current_topic = topic.clone();
                                        }
                                    }
                                });
                            });
                        }
                    }
                });
            }
        });

        // 主窗口
        egui::CentralPanel::default().show(ctx, |ui| {
            match self.panel_id.as_str() {
                "list" => {
                // 数据页面
                //     数据页面的菜单tab按钮
                    egui::TopBottomPanel::top("list_top_panel").show(ctx, |ui| {
                        ui.horizontal(|ui| {
                            if ui.button("配置").clicked() {
                                self.list_panel_id = Some("configuration".to_owned());
                            };
                            if ui.button("数据").clicked() {
                                self.list_panel_id = Some("data".to_owned());
                            };
                        });
                    });
                    ui.horizontal(|ui| {
                        match &self.list_panel_id {
                            Some(id) => {
                                match id.as_str() {
                                    "configuration" => {
                                        egui::CentralPanel::default().show(ctx, |ui| {
                                            ui.horizontal(|ui| {
                                                ui.label("配置页");
                                            });
                                        });
                                    }
                                    _ => {
                                        egui::CentralPanel::default().show(ctx, |ui| {
                                            ui.horizontal(|ui| {
                                                ui.label("起点位置:");
                                                egui::ComboBox::from_label("")
                                                    .selected_text(self.current_offset_type.as_str())
                                                    .show_ui(ui, |ui| {
                                                        ui.selectable_value(&mut self.current_offset_type, "起始".to_owned(), "起始");
                                                        ui.selectable_value(&mut self.current_offset_type, "最新".to_owned(), "最新");
                                                    },
                                                    );

                                                ui.label("  ");
                                                ui.label("拉取数量:");
                                                ui.add(egui::DragValue::new(
                                                    &mut self.current_config.poll_rows,
                                                ));
                                                if ui.button("拉取").clicked() {
                                                    let hosts = self.current_config.host.split(",").map(|h| h.to_owned()).collect();
                                                    self.partition_offsets.clear();

                                                    let mut client = KafkaClient::new(hosts);
                                                    client.set_group_offset_storage(Some(GroupOffsetStorage::Kafka));
                                                    client.load_metadata(&vec![self.current_topic.clone()]).unwrap();

                                                    match self.current_offset_type.as_str() {
                                                        "起始" => {
                                                            let topic_partition_offset = client.fetch_offsets(&vec![self.current_topic.clone()], FetchOffset::Earliest).unwrap();
                                                            let partition_offsets = topic_partition_offset.get(self.current_topic.as_str()).unwrap();
                                                            for po in partition_offsets {
                                                                client.commit_offset(KAFKA_GROUP_ID, self.current_topic.as_str(), po.partition, po.offset).unwrap();
                                                            }
                                                        }
                                                        _ => {
                                                            let topic_partition_offset = client.fetch_offsets(&vec![self.current_topic.clone()], FetchOffset::Latest).unwrap();
                                                            let partition_offsets = topic_partition_offset.get(self.current_topic.as_str()).unwrap();

                                                            let sub_count: i64 = if partition_offsets.len() > 0 {
                                                                self.current_config.poll_rows / partition_offsets.len()
                                                            } else {
                                                                self.current_config.poll_rows
                                                            } as i64;

                                                            for po in partition_offsets {
                                                                let mut offset = po.offset - sub_count;
                                                                if offset < 0 {
                                                                    offset = 0;
                                                                }
                                                                client.commit_offset(KAFKA_GROUP_ID, self.current_topic.as_str(), po.partition, offset).unwrap();
                                                            }
                                                        }
                                                    };
                                                    let mut messages = vec![];
                                                    let mut reqs = vec![];
                                                    for po in &self.partition_offsets {
                                                        reqs.push((po.partition, po.offset))
                                                    }
                                                    let mut consumer = Consumer::from_client(client).with_topic(self.current_topic.clone()).with_group(KAFKA_GROUP_ID.to_string())
                                                        .with_fallback_offset(
                                                            match self.current_offset_type.as_str() {
                                                                "起始" => {
                                                                    FetchOffset::Earliest
                                                                }
                                                                _ => {
                                                                    FetchOffset::Latest
                                                                }
                                                            }
                                                        )
                                                        .create().unwrap();
                                                    loop {
                                                        let ms = &consumer.poll().unwrap();
                                                        if ms.is_empty() {
                                                            break;
                                                        }
                                                        for ms in ms.iter() {
                                                            for msg in ms.messages() {
                                                                messages.push(KafkaMessage {
                                                                    offset: msg.offset.to_owned(),
                                                                    key: String::from_utf8_lossy(&msg.key).parse().unwrap(),
                                                                    value: String::from_utf8_lossy(&msg.value).parse().unwrap(),
                                                                });
                                                            }
                                                            consumer.consume_messageset(ms).unwrap();
                                                        }
                                                        consumer.commit_consumed().unwrap();
                                                        if messages.len() == self.current_config.poll_rows {
                                                            break;
                                                        }
                                                    }
                                                    self.current_messages = messages;
                                                }
                                            });

                                            let mut table = TableBuilder::new(ui)
                                                .striped(true)
                                                .resizable(true)
                                                .cell_layout(egui::Layout::left_to_right(egui::Align::Center))
                                                .column(Column::auto())
                                                .column(Column::initial(100.0).range(40.0..=300.0))
                                                .column(Column::initial(100.0).at_least(40.0).clip(true))
                                                .column(Column::remainder())
                                                .min_scrolled_height(0.0);
                                            table
                                                .header(20.0, |mut header| {
                                                    header.col(|ui| {
                                                        ui.label("序列");
                                                    });
                                                    header.col(|ui| {
                                                        ui.label("偏移量");
                                                    });
                                                    header.col(|ui| {
                                                        ui.label("键");
                                                    });
                                                    header.col(|ui| {
                                                        ui.label("值");
                                                        ui.text_edit_singleline(&mut self.value_filter);
                                                    });
                                                }).body(|mut body| {
                                                for (index, km) in self.current_messages.iter().enumerate() {
                                                    if km.value.contains(&self.value_filter) || self.value_filter.trim().is_empty() {
                                                        body.row(30.0, |mut row| {
                                                            row.col(|ui| {
                                                                ui.label((index + 1).to_string());
                                                            });
                                                            row.col(|ui| {
                                                                ui.label(format!("{}", km.offset.clone()));
                                                            });
                                                            row.col(|ui| {
                                                                ui.label(km.key.clone());
                                                            });
                                                            row.col(|ui| {
                                                                ui.label(km.value.clone());
                                                            });
                                                        });
                                                    }
                                                }
                                            });
                                        });
                                    }
                                }
                            }
                            None => {}
                        }
                    });
                }
                _ => {
                    // 新键页面
                    ui.horizontal(|ui| {
                        ui.label("分组:");
                        ui.text_edit_singleline(&mut self.temp_config.group_name);
                    });
                    ui.horizontal(|ui| {
                        ui.label("名称:");
                        ui.text_edit_singleline(&mut self.temp_config.name);
                    });
                    ui.horizontal(|ui| {
                        ui.label("地址:");
                        ui.text_edit_singleline(&mut self.temp_config.host);
                    });
                    match &self.temp_config.message {
                        Some(message) => {
                            ui.label(message);
                        }
                        None => {
                            ui.label("");
                        }
                    }
                    ui.horizontal(|ui| {
                        if ui.button("保存").clicked() {
                            if self.temp_config.id.is_empty() {
                                self.temp_config.id = uuid::Uuid::new_v4().to_string();
                            }
                            self.config.kafka_configs.push(self.temp_config.clone());
                            self.save_config();
                            ui.horizontal(|ui| {
                                ui.label(format!("保存成功!"));
                            });
                        }
                        if ui.button("测试").clicked() {
                            let config = &self.temp_config;
                            let host = &config.host;
                            let mut client = KafkaClient::new(
                                host.split(",").map(|h| h.to_string()).collect(),
                            );
                            match client.load_metadata_all() {
                                Ok(_) => {
                                    let topics: Vec<String> =
                                        client.topics().names().map(ToOwned::to_owned).collect();
                                    self.temp_config.topics = topics;
                                    self.temp_config.message = Some("连接成功!".to_owned())
                                }
                                Err(e) => {
                                    self.temp_config.message =
                                        Some(format!("连接失败: {}!", e.to_string()).to_string());
                                }
                            };
                        };
                    });
                }
            }
        });
    }
}

fn main() -> eframe::Result<()> {
    let options = eframe::NativeOptions::default();


    eframe::run_native(
        APP_NAME,
        options,
        Box::new(|cc| Box::<ToolApp>::new(ToolApp::new(cc))),
    )
}
