package io.github.rdt.console;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** RDT 独立管理控制台。 */
@SpringBootApplication
public class RdtConsoleApplication {
    /** 创建应用配置。 */
    public RdtConsoleApplication() { }

    /**
     * 启动独立控制台。
     *
     * @param args Spring Boot 参数
     */
    public static void main(String[] args) {
        SpringApplication.run(RdtConsoleApplication.class, args);
    }
}
