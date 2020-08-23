package com.windTa1ker.spring;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * time: 2020/8/23 6:52 下午
 * author: 荆旗
 * Description:
 */
@SpringBootApplication
@Slf4j
public class DataSourceDemoApplicationBackup implements CommandLineRunner {
  @Autowired
  private DataSource dataSource;
  
  public static void main(String[] args) {
    SpringApplication.run(DataSourceDemoApplicationBackup.class, args);
  }
  
  @Override
  public void run(String... args) throws Exception {
    showConnection();
  }
  
  private void showConnection() throws SQLException {
    log.info(dataSource.toString());
    Connection conn = dataSource.getConnection();
    log.info(conn.toString());
    log.info(conn.getSchema());
    conn.close();
  }
  
}
