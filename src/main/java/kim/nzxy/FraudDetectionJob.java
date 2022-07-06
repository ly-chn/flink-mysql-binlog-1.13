/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kim.nzxy;

import com.ververica.cdc.connectors.mysql.MySqlSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * 定义程序的数据流
 */
public class FraudDetectionJob {
    public static void main(String[] args) throws Exception {
        SourceFunction<String> sourceFunction = MySqlSource.<String>builder()
                .hostname("192.168.xxx.xxx")
                .port(3306)
                .databaseList("flink_demo") // 订阅的库
                .username("root")
                .password("password")
                .deserializer(new JsonDebeziumDeserializationSchema() )
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> mysql = env.addSource(sourceFunction)
                .setParallelism(1)
                .name("mysql");
        mysql.keyBy(s->1)
                .process(new ProcessForLogMysqlChange())
                .name("mysql2");
        env.execute();
    }
}
