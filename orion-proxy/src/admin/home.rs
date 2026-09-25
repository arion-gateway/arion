// Copyright 2025-2026 The arion-gateway Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::admin::AdminState;
use axum::{extract::State, response::Html};
use maud::{html, Markup, DOCTYPE};

pub async fn home_handler(State(mut _admin_state): State<AdminState>) -> Html<String> {
    // Define minimal colors
    const COLOR_BG: &str = "#0a0a0a";
    const COLOR_FG_HIGH: &str = "#ffffff";
    const COLOR_FG_MED: &str = "#a0a0a0";
    const COLOR_NEON: &str = "#00ff41";
    const COLOR_BORDER: &str = "#333333";

    // Generate the HTML table using Maud with improved button styling
    let markup: Markup = html! {
        (DOCTYPE)
        html lang="en" {
            head {
                meta charset="UTF-8";
                title { "Orion Admin | Console" }
                style {
                    // Minimalist reset
                    "body { font-family: 'Courier New', Courier, monospace; margin: 2rem; background-color: " (COLOR_BG) "; color: " (COLOR_FG_HIGH) "; }"

                    // Simple neon title without shadows
                    "h2 { color: " (COLOR_NEON) "; text-transform: uppercase; margin-bottom: 1.5rem; }"

                    // Flat table layout with only bottom borders
                    "table { border-collapse: collapse; width: 100%; max-width: 1000px; }"
                    "th, td { border-bottom: 1px solid " (COLOR_BORDER) "; padding: 10px 0; text-align: left; vertical-align: middle; }"
                    "th { color: " (COLOR_FG_HIGH) "; text-transform: uppercase; font-size: 0.9rem; }"

                    // Column sizing and text colors
                    "td.command-cell { font-weight: bold; width: 30%; }"
                    "td.desc-cell { color: " (COLOR_FG_MED) "; }"

                    // Simple hover effects for links
                    "a { text-decoration: none; color: " (COLOR_FG_HIGH) "; }"
                    "a:hover { color: " (COLOR_NEON) "; }"

                    // Button alignment and minimalist styling
                    "form { margin: 0; display: inline-block; vertical-align: middle; }"
                    "button { cursor: pointer; font-family: inherit; font-size: 0.9rem; font-weight: bold;"
                             "padding: 4px 8px; background-color: #1a1a1a; border: 1px solid " (COLOR_BORDER) "; color: " (COLOR_FG_HIGH) "; }"
                    "button:hover { color: " (COLOR_NEON) "; border-color: " (COLOR_NEON) "; }"
                }
            }
            body {
                h2 { "> Orion_Admin::Console" }
                table {
                    thead {
                        tr { th { "Command" } th { "Description" } }
                    }
                    tbody {
                        tr { td class="command-cell" { a href="/" { "/" } } td class="desc-cell" { "admin home page" } }
                        tr { td class="command-cell" { a href="/certs" { "/certs" } } td class="desc-cell" { "print certs on machine" } }
                        tr { td class="command-cell" { a href="/clusters" { "/clusters" } } td class="desc-cell" { "upstream cluster status" } }

                        @if cfg!(feature = "config-dump") {
                            tr { td class="command-cell" { a href="/config_dump" { "/config_dump" } } td class="desc-cell" { "dump current Orion configs" } }
                        }

                        tr { td class="command-cell" { a href="/help" { "/help" } } td class="desc-cell" { "print out list of admin commands" } }
                        tr { td class="command-cell" { a href="/listeners" { "/listeners" } } td class="desc-cell" { "print listener info" } }
                        tr { td class="command-cell" { a href="/memory" { "/memory" } } td class="desc-cell" { "print current allocation/heap usage" } }
                        tr { td class="command-cell" { a href="/ready" { "/ready" } } td class="desc-cell" { "print server state" } }

                        @if cfg!(feature = "metrics") {
                            tr {
                                td class="command-cell" {
                                    form action="/reset_counters" method="post" {
                                        button type="submit" { "/reset_counters" }
                                    }
                                }
                                td class="desc-cell" { "reset all counters to zero" }
                            }
                        }

                        tr { td class="command-cell" { a href="/server_info" { "/server_info" } } td class="desc-cell" { "print server version/status information" } }

                        @if cfg!(feature = "metrics") {
                            tr { td class="command-cell" { a href="/stats" { "/stats" } } td class="desc-cell" { "print server stats" } }
                        }

                        @if cfg!(feature = "prometheus") {
                            tr { td class="command-cell" { a href="/stats/prometheus" { "/stats/prometheus" } } td class="desc-cell" { "print server stats in prometheus format" } }
                        }
                    }
                }
            }
        }
    };

    Html(markup.into_string())
}
