# Koi UI source provenance

上游来源：本机 `/Users/ren/Desktop/koi-ui-master`，2026-10-19。许可证保存在 `../../KOI-LICENSE`。

`source/` 保存未修改的原始布局、Header/Toolbar、主题面板、登录和 Home/Menu/Logo/ScrollNav 源码用于追溯。活动 `src/layouts`、`views/auth/Login.vue` 直接采用原工程模板与样式，脚本改为 RDT 的真实身份、权限、菜单和运行数据；不是只在 vendor 放参考文件。Home 移除假图表和假同比，保留卡片布局；品牌用自有 `/rdt-mark.svg`。

原始文件 SHA-256：

- `source/components/KoiScrollNav/Index.vue`: `b5608608e5203bed16b9ff8d80f862f90a082885247e75df3a424b02e4f290f4`
- `source/layouts/LayoutClassic/index.vue`: `efca7832f472e5dc5bd7cdafef2a6b7ff4aad90324f7178a718bc44a7a23e450`
- `source/layouts/LayoutColumns/index.vue`: `6dbddb10acffec298e7173626768181df8e14e1fb45a358f585dfc854751af58`
- `source/layouts/LayoutHorizontal/index.vue`: `c92a2dae8c61dfa4cebebe849c6605aaf5083b766782b6a1c3222c0673a52663`
- `source/layouts/LayoutMobile/index.vue`: `aaa0ea6c033429632933faf6955c1ede24691b9dd65d23ece625ce0bb762a7e9`
- `source/layouts/LayoutOptimum/index.vue`: `45adadbb5dd5e1074aa43bbb33453161cab19a3864162212da718545fea962dd`
- `source/layouts/LayoutVertical/index.vue`: `977fd9617e30bf19dc1c5baa19d3776a52d05ccaf22362a3ff23adb4643bf3e4`
- `source/layouts/components/Header/components/BreadCrumb.vue`: `448f3de0626c67b8a4d7b50a8f746c744c6a4f15aeef413f142b44220bd987d3`
- `source/layouts/components/Header/components/Collapse.vue`: `2278962af8879434e69b2473f70fa684b8be7cca0d50773dcf6a38805cc66eb8`
- `source/layouts/components/Header/components/Dark.vue`: `ccc8e4cb281840ccda9326f15a822c7dd3d7883fb1c8a8c921efb55ac4770b14`
- `source/layouts/components/Header/components/Dimension.vue`: `66858571714bf775c1f830b94283c4154a57714c5542d05eea8e1914d9794977`
- `source/layouts/components/Header/components/FullScreen.vue`: `f391f55cbd84671627ac6bdaad804e17486663ba47d57426bc0526012135b939`
- `source/layouts/components/Header/components/Language.vue`: `2f74bb7f62d32509bd2283e0fc632b0727e22e7db661ce21355d5daa9e857f0b`
- `source/layouts/components/Header/components/Refresh.vue`: `c8cabcb970469da03f2d14686ff4fcfcecd556769e93d0362a2490f6582c58d4`
- `source/layouts/components/Header/components/SearchMenu.vue`: `a06ce71541f9967952643dd90aa2bfb60ed9694c513451b994ff41c24da706b7`
- `source/layouts/components/Header/components/ThemeSetting.vue`: `84f13a5ba2ae2cc539b2427dec99171d21e76ce641a2c534773b42ba85c8467b`
- `source/layouts/components/Header/components/Toolbar.vue`: `0a511b63b0d879af5e6b65bd4e548cdcfff3fc153acff55626be26a27cc0f823`
- `source/layouts/components/Header/components/User.vue`: `19845144880d88abc37d2960fbbb1e62f65e953c1f03fbcc551f00b379b99905`
- `source/layouts/components/Header/index.vue`: `f203f824a85aa15358310170fcbd69e3f983bf6578b929bb7999a422f7963fdd`
- `source/layouts/components/Logo/index.vue`: `1e2b53bb0a97cb63f2b13786a8141c0d77e2fcb42c6b08923ae6923f90cbf0b2`
- `source/layouts/components/Main/components/Maximize.vue`: `2ea182934927a6ff257e44b44a173ab9f4cfad2a3748671b408396ec1cf8ff3e`
- `source/layouts/components/Main/index.vue`: `7eb1026ad6bcb48139c0a81504135c538e3999a57fd4531c4a20f0cab9130467`
- `source/layouts/components/Menu/AsideSubMenu.vue`: `bc7ec11d52a7d43b4cedb005648a438d943b5f436914e0bf9008658ecc148c4f`
- `source/layouts/components/Menu/ColumnSubMenu.vue`: `1e6c40cdb468b321fc218f875c07731693709f46089380c9bf781f376af3f647`
- `source/layouts/components/Menu/HorizontalSubMenu.vue`: `590f6d01e304d80fc84d87c7dce9336935a1a2fe4fd896cc6385c69161335218`
- `source/layouts/components/Tabs/components/TabMenu.vue`: `4efc3fd636ae0a7741350924567ffdcf54fedc60649db3c08fa47563a61b605a`
- `source/layouts/components/Tabs/index.vue`: `38e5eb5c467f110e8d039b43a85d3cc61f555b4ce0e31d153c5fbd90782ea04d`
- `source/layouts/components/Tabs/useLayoutTabs.ts`: `4dfdfabbd5001a3ec6b096708f0af28b6ba82bffa7fb9f7ccc685dfe4d76e8aa`
- `source/layouts/components/ThemeConfig/index.vue`: `9fb0eded18cf6bd26b112736bec9c82acfdd5a107620df5520805194c7f7b0f5`
- `source/layouts/index.vue`: `96a0047a8c8bb0965f0de649be50d219f807a2a032c1689393ca5aa45e4be9e0`
- `source/login/components/KoiLoading.vue`: `833401d998fb2943a432b125dd17dd491147e9166e7449497d87ef29431b8a84`
- `source/login/components/KoiThemeColor.vue`: `6aa27517e96c1149217cc3529cbfec04c53c48123caabbf16d8e2a60f8c7423e`
- `source/login/index.vue`: `4f8a0a5ce5017bee76cf31bfc5f1a97f694aeb3942aaa3b0dbe5c2373ab163d4`
- `source/views/home/components/HomeModulePieChart.vue`: `ab2425f881f22a4927eb1a832c4f32f0f6dde6706049718beee9ed32f63c39f8`
- `source/views/home/components/HomeProjectAbout.vue`: `57ce4739fadd2c3f725ad1356887c4e8e8bc515eed909d1c037f0c5ad1282178`
- `source/views/home/components/HomeStatCards.vue`: `38a207290bab74cc23db4758cdda3a1e1bbb723ca6f439bd380cac03ca4e1603`
- `source/views/home/components/HomeTradeLineChart.vue`: `80f0e6b11b87400348a7a13cb4999733cb95cb47dd89bbb290be08937b13b108`
- `source/views/home/components/HomeUserBarChart.vue`: `3c72b65dd09f9c8fcd68dccfa197c1111596740ad67f9e595334b5b4f9afee86`
- `source/views/home/components/HomeVisitLineChart.vue`: `80fc4f07242f612a284aae6b73b7130b5c2fefa1d94039585b281ed98e041103`
- `source/views/home/index.vue`: `ce3173efc84c944b52f40fe6f205a7090d01c48806cc6b77ea707b089bd2b908`
