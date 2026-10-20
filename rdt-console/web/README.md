# RDT Console Web

Console 内置的 Vue 3 + Koi UI 前端，不是单独部署的服务。运行一个可执行 Console JAR 即可同时使用页面和管理 API，默认地址 `http://127.0.0.1:9898`。

## 单 JAR 构建与启动

在 RDT 仓库根目录执行：

```bash
sh mvnw -pl rdt-console -am clean package
sh scripts/console.sh
```

构建机需要 JDK 21、Node >=20.19 和 pnpm。Maven 在 prepare-package 阶段执行 `pnpm install --frozen-lockfile` 和 `pnpm run build`，类型检查或构建失败立即终止打包。Vite 清理并写入 `target/generated-resources/console/static`，构建脚本同步到 `target/classes/static`；生成目录同时注册为 Maven/IDE 资源根，避免 IDE 重编译后丢首页。之后 Spring Boot 将当前 JS/CSS/字体打进可执行 JAR。不读取历史 `web/dist`。

运行机只需要 Java（管理 Redis 时再配置连接），不启动 Node/Vite。可单独复制 `target` 下的 JAR 和 `jvm.options`，执行 `java @jvm.options -jar rdt-console-1.0.0-SNAPSHOT.jar`。Console 启动不依赖 Redis；登录后在“连接设置”分别配置 runtime/cache，消息/任务等只用 runtime，cache 当前仅支持配置和连通性测试。账号与资源参数见根 README；默认启动脚本使用 32–256 MB 堆与 Serial GC，该堆上限不是进程总内存限制。所有页面与 API 同源；刷新 `/tasks` 等路由时，服务端内部转发到包内首页，不向其他端口重定向。

## Koi UI

活动路由使用上游五种布局（纵向、经典、横向、分栏、综合）、移动端抽屉、完整右上角工具栏、ThemeConfig、登录页动效、菜单/页签、KoiScrollNav、字体、主题与 UnoCSS。首页使用上游 Home 欢迎区和统计卡片结构，品牌为自有 RDT SVG。保留真实 RDT 登录、namespace 选择、分页和二次确认，不使用上游演示数据。来源见 `vendor/koi-ui/README.md`，许可证见 `KOI-LICENSE`。

图标直接使用 Koi 原版 `src/assets/icons`，通过 SVG sprite 注册；菜单和右上角工具沿用 Koi 图标与 hover 动效。折叠菜单的图标保留在默认插槽，文字放入标题插槽。工具栏以固定内容宽度过渡展开/收起，并支持减少动态效果的系统偏好。管理表格使用 Koi 的边框、居中表头、固定操作列和行间距；名称与稳定 ID 在同一列分行呈现。

首页复用上游 ECharts 的线图、渐变柱图和环图风格，数据全部来自当前命名空间管理 API：曲线只保留本次页面打开后最近 30 次成功的完整快照；主题柱图展示当前加载前 50 个主题中保留消息最多的 8 个；节点环图按正常接单、压力保护、停止接单互斥分类。读取失败显示不可用，不把旧结果当作新采样；主题保留数不作为消费积压量或吞吐率。切换命名空间清空观察记录，离开页面释放图表与 ResizeObserver。

页面术语参考 [RocketMQ 领域模型](https://rocketmq.apache.org/zh/docs/domainModel/01main/) 中的 Topic、ConsumerGroup、Consumer，以及 [PowerJob JobInfoDO](https://github.com/PowerJob/PowerJob/blob/master/powerjob-server/powerjob-server-persistence/src/main/java/tech/powerjob/server/persistence/remote/model/JobInfoDO.java) / [InstanceInfoDO](https://github.com/PowerJob/PowerJob/blob/master/powerjob-server/powerjob-server-persistence/src/main/java/tech/powerjob/server/persistence/remote/model/InstanceInfoDO.java) 对任务定义、任务实例和执行次数的区分。仅对齐展示概念，状态含义以 RDT 实现为准：

| 页面名称 | RDT 字段 | 含义 |
| --- | --- | --- |
| 消息主题（Topic） | destination | 业务声明的主题标识，替代“目的地” |
| 消费组名称 | group / name | 同一消费身份下的消费者分组 |
| 保留消息数 | length | Redis Stream 当前仍保留的条目数，可能含已消费消息 |
| 待确认 | pending | 已投递给消费组但尚未确认的消息 |
| 未投递 | lag | 尚未投递给该消费组的消息；负值表示 Redis 未提供 |
| 任务名称 / ID | taskId | 调度任务的稳定身份 |
| 执行实例 ID | executionId | 一次触发创建的执行实例；失败重试属于同一次执行实例 |
| 执行次数 | attempt | 该执行实例已执行的次数，包含重试 |
| 等待可用节点 | WAITING | 当前没有健康且有容量的执行节点 |
| 运行节点 / 执行节点 | instanceId | 业务进程使用的稳定节点身份 |
| 进程运行 ID | runId | 一次进程运行的身份，用于区分节点重启 |


修改前端源码后重新 package 并重启 JAR；运行中的旧 JAR 不会自动更新。IDE 重建可从 generated-resources 恢复首页；执行 clean 后要重新 package。缺前端资源时启动明确失败并提示构建命令。

## 验证

在当前 web 目录执行：

```bash
pnpm exec vue-tsc --noEmit
pnpm lint
# 已启动的测试 JAR：页面和真实管理 API 均走同一个端口，不启动 Vite。
PLAYWRIGHT_BASE_URL=http://127.0.0.1:9898 PLAYWRIGHT_REAL_API=1 pnpm test:ui
```

真实测试使用 `RDT_CONSOLE_USERNAME` / `RDT_CONSOLE_PASSWORD`，这些环境变量可覆盖测试凭据，默认使用产品首次创建的 rdt / rdt；先通过连接设置保存专用 runtime Redis，测试 Redis 中需有可发现的 RDT namespace。Playwright Chromium 需预先安装，也可设置 `PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH` 指向本机 Chrome。

夹具测试模拟管理 API，覆盖五种布局、折叠菜单 SVG、工具栏宽度动画、指标图表、表格语义、移动端和操作权限；额外的单 JAR 测试实际登录并访问全部管理页、校验同源和无重定向。它们都不替代后端 Redis/JDBC 的故障注入。

`pnpm dev` 仅是可选的源码热更新工具，使用 3010 并代理 `/rdt` 到 9898；不是 Console 的启动/部署依赖，后端不会跳转过去。不设置 `PLAYWRIGHT_BASE_URL` 时，夹具测试自行启动这项开发工具，真实 API 测试跳过。

连接设置专项浏览器测试会修改目标配置，只在专用测试 Console 上独立运行：

```bash
PLAYWRIGHT_BASE_URL=http://127.0.0.1:9898 PLAYWRIGHT_CONNECTION_SETTINGS=1 \
  pnpm exec playwright test connection-settings.spec.ts
```

可用 `PLAYWRIGHT_REDIS_ADDRESS` 指定测试 Redis，默认 `redis://127.0.0.1:17379`。该测试从无目标登录开始，覆盖不保存的连通性测试、runtime DB0/cache DB1 独立配置、保存确认、刷新与进入概览，不与其他真实管理测试并行执行。


本地认证不依赖 Redis 或企业数据库。首启自动创建最高权限账号 `rdt / rdt`，直接登录，无须注册。后续启动不覆盖账号文件或重置改过的密码。`security.json` 保存加盐密码哈希和会话摘要，管理员可在“用户与权限”分配 ADMIN / OPERATOR / VIEWER；改权限、禁用和改密码会立即撤销会话。不在 YAML 中配置 username/password/token-secret。详见根 README“本地身份、角色与持久化”。
