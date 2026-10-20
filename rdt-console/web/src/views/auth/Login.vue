<template>
  <div class="login-page w-screen h-screen overflow-hidden" data-ui="koi-login">
    <el-row class="h-100%">
      <!-- 登录工具栏 -->
      <div
        class="flex flex-items-center pos-absolute top-8px right-8px z-10 h-40px p-y-2px p-x-12px bg-#F4F4F5 dark:bg-#141414 border-1px border-solid border-#E4E7ED dark:border-#4C4D4F rounded-20px shadow-[0_4px_12px_rgb(0_0_0_/_15%)] dark:shadow-[0_4px_12px_rgba(255,255,255,0.1)] transition-all transition-300ms transition-ease"
      >
        <KoiThemeColor></KoiThemeColor>
        <KoiLanguage></KoiLanguage>
        <KoiDark></KoiDark>
      </div>

      <el-col :lg="16" :md="12" :sm="15" :xs="0" class="flex flex-items-center flex-justify-center">
        <div class="login-background w-100% h-100%">
          <!-- 动态装饰光斑 -->
          <div class="bg-shape bg-shape--1"></div>
          <div class="bg-shape bg-shape--2"></div>
          <div class="bg-shape bg-shape--3"></div>

          <!-- 毛玻璃覆盖层 -->
          <div class="glass-overlay"></div>

          <!-- 内容层 -->
          <div class="pos-absolute text-center select-none transition-all transition-ease transition-500 content-layer">
            <div class="brand-badge flex flex-items-center flex-justify-center gap-10px m-b-32px <md:hidden">
              <div class="login-logo-wrap login-logo-wrap--brand">
                <img class="login-logo" :src="logo" alt="RDT Console" />
              </div>
              <span class="brand-text text-18px font-700">RDT Console</span>
            </div>
            <el-image
              class="w-260px max-w-500px h-260px m-b-40px animate-float-picture <md:hidden <lg:h-320px <lg:max-w-400px"
              :src="science"
            />
            <div class="welcome-title text-2xl font-700 m-b-12px text-center <lg:text-xl <md:hidden">
              Redis Distributed Toolkit
            </div>
            <div class="welcome-subtitle text-28px font-800 m-b-16px text-center <lg:text-22px <md:hidden">
              RDT Console
            </div>
            <div class="welcome-desc text-16px font-400 text-center max-w-420px mx-auto leading-relaxed <md:hidden">
              统一管理 Redis 分布式组件，连接、账号和权限独立存于本地。
            </div>
            <div class="feature-tags flex flex-justify-center gap-12px m-t-32px flex-wrap <md:hidden">
              <span class="feature-tag">
                <el-icon :size="14" class="feature-tag-icon"><Promotion /></el-icon>
                高效管理
              </span>
              <span class="feature-tag">
                <el-icon :size="14" class="feature-tag-icon"><Brush /></el-icon>
                现代设计
              </span>
              <span class="feature-tag">
                <el-icon :size="14" class="feature-tag-icon"><Lock /></el-icon>
                安全可靠
              </span>
            </div>
          </div>

          <div class="bei-an-hao select-none &lt;md:hidden">Redis Distributed Toolkit · Standalone Console</div>
        </div>
      </el-col>

      <el-col
        :lg="8"
        :md="12"
        :sm="9"
        :xs="24"
        class="login-form-side flex flex-items-center flex-justify-center flex-col"
      >
        <div class="login-form-panel w-100% flex flex-col flex-items-center">
          <!-- 移动端 Logo -->
          <div class="login-mobile-brand md:hidden">
            <div class="login-logo-wrap login-logo-wrap--mobile">
              <img class="login-logo" :src="logo" alt="RDT Console" />
            </div>
            <div class="font-600 text-xl">RDT Console</div>
          </div>

          <div class="form-header text-center m-b-32px">
            <h3 class="text-24px font-700 m-b-8px text-[--el-text-color-primary]">
              账号登录
            </h3>
            <p class="text-14px text-[--el-text-color-regular]">
              {{ $t("menu.login.form.loginName") }} /
              {{ $t("menu.login.form.password") }}
            </p>
          </div>

          <el-form
            ref="loginFormRef"
            :model="loginForm"
            :rules="loginRules"
            class="login-form w-300px"
            @submit.prevent="handleKoiLogin"
            @keydown.enter.prevent="handleKoiLogin"
          >
            <el-form-item prop="loginName">
              <el-input
                v-model="loginForm.loginName"
                type="text"
                placeholder="管理账号"
                size="large"
                class="login-input"
              >
                <template #prefix>
                  <el-icon :size="16"><User /></el-icon>
                </template>
              </el-input>
            </el-form-item>

            <el-form-item prop="password">
              <el-input
                v-model="loginForm.password"
                type="password"
                placeholder="管理密码"
                show-password
                size="large"
                class="login-input"
              >
                <template #prefix>
                  <el-icon :size="16"><Lock /></el-icon>
                </template>
              </el-input>
            </el-form-item>

            <el-alert v-if="error" :title="error" type="error" :closable="false" class="m-b-16px" />
            <el-form-item class="m-b-0">
              <el-button
                type="primary"
                class="login-btn w-100% tracking-4px"
                size="large"
                :loading="loading"
                @click="handleKoiLogin"
              >
                {{ loading ? "正在处理" : "登录" }}
              </el-button>
            </el-form-item>

            <p class="setup-hint text-center">
              默认管理员 rdt / rdt · Redis 离线仍可登录
            </p>
          </el-form>
        </div>
      </el-col>
    </el-row>

    <ThemeConfig />
  </div>
</template>

<script setup lang="ts">
import { reactive, ref } from "vue";
import { useRouter, useRoute } from "vue-router";
import { type FormInstance, type FormRules } from "element-plus";
import { User, Lock, Promotion, Brush } from "@element-plus/icons-vue";
import KoiThemeColor from "@/layouts/components/Header/components/ThemeSetting.vue";
import KoiLanguage from "@/layouts/components/Header/components/Language.vue";
import KoiDark from "@/layouts/components/Header/components/Dark.vue";
import ThemeConfig from "@/layouts/components/ThemeConfig/index.vue";
import { useTheme } from "@/utils/theme";
import { useAuthStore } from "@/stores/auth";
import { apiError } from "@/api/security";
const logo = "/rdt-mark.svg";
const science = "/static/koi-login-science.png";
const auth = useAuthStore();
const router = useRouter();
const route = useRoute();
useTheme();
const loading = ref(false);
const error = ref("");
const loginFormRef = ref<FormInstance>();
const loginForm = reactive({
  loginName: "",
  password: ""
});
const loginRules: FormRules = {
  loginName: [{ required: true, message: "请输入账号", trigger: "blur" }],
  password: [{ required: true, message: "请输入密码", trigger: "blur" }]
};
async function handleKoiLogin() {
  if (loading.value || !(await loginFormRef.value?.validate())) return;
  loading.value = true;
  error.value = "";
  try {
    await auth.login(loginForm.loginName, loginForm.password);
    const target =
      typeof route.query.redirect === "string" &&
      route.query.redirect.startsWith("/") &&
      !route.query.redirect.startsWith("//")
        ? route.query.redirect
        : "/overview";
    await router.push(target);
  } catch (failure) {
    error.value = apiError(failure, "登录失败，请检查账号和密码");
  } finally {
    loading.value = false;
  }
}
</script>

<style lang="scss" scoped>
/** 备案号 */
.bei-an-hao {
  position: absolute !important;
  bottom: 0 !important;
  left: 50% !important;
  transform: translateX(-50%) !important;
  font-size: 12px;
  font-weight: normal;
  text-align: center;
  z-index: 10 !important;
  white-space: nowrap;
  padding-bottom: 10px;
  width: 100%;
}

.bei-an-hao a {
  font-size: 12px;
  opacity: 0.7;
  transition: opacity 0.3s;

  &:hover {
    opacity: 1;
  }
}

/* 左侧背景 */
.login-background {
  position: relative;
  overflow: hidden;
  background:
    radial-gradient(ellipse 600px 450px at 85% 20%, rgba(var(--el-color-primary-rgb), 0.12), transparent 70%),
    radial-gradient(500px circle at 25% 80%, rgba(var(--el-color-primary-rgb), 0.1), transparent 65%),
    radial-gradient(350px circle at 50% 50%, rgba(var(--el-color-primary-rgb), 0.08), transparent 60%),
    var(--el-bg-color-page, #f8f8f8);
}

html.dark .login-background {
  background:
    radial-gradient(ellipse 600px 450px at 85% 20%, rgba(var(--el-color-primary-rgb), 0.25), transparent 70%),
    radial-gradient(500px circle at 25% 80%, rgba(var(--el-color-primary-rgb), 0.2), transparent 65%),
    radial-gradient(350px circle at 50% 50%, rgba(var(--el-color-primary-rgb), 0.15), transparent 60%), #03020c;
}

/* 动态光斑 */
.bg-shape {
  position: absolute;
  border-radius: 50%;
  filter: blur(60px);
  opacity: 0.5;
  pointer-events: none;
  animation: bg-float 18s infinite ease-in-out;
  z-index: 0;

  &--1 {
    top: -5%;
    left: 10%;
    width: 400px;
    height: 400px;
    background: rgba(var(--el-color-primary-rgb), 0.35);
  }

  &--2 {
    bottom: 5%;
    right: 5%;
    width: 300px;
    height: 300px;
    background: color-mix(in srgb, var(--el-color-primary) 50%, #a855f7 50%);
    animation-delay: -6s;
  }

  &--3 {
    top: 40%;
    left: 50%;
    width: 200px;
    height: 200px;
    background: color-mix(in srgb, var(--el-color-primary) 50%, #06b6d4 50%);
    animation-delay: -12s;
  }
}

html.dark .bg-shape {
  opacity: 0.3;
}

/* 毛玻璃覆盖层 */
.glass-overlay {
  position: absolute;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  background: rgba(255, 255, 255, 0.06);
  backdrop-filter: blur(40px);
  -webkit-backdrop-filter: blur(40px);
  border-right: 1px solid rgba(255, 255, 255, 0.15);
  z-index: 1;
  pointer-events: none;
}

html.dark .glass-overlay {
  background: rgba(0, 0, 0, 0.25);
  border-right: 1px solid rgba(255, 255, 255, 0.08);
}

/* 内容层 */
.content-layer {
  position: absolute;
  z-index: 2;
  top: 50%;
  left: 50%;
  transform: translate(-50%, -50%);
  width: 100%;
  padding: 0 40px;
  text-align: center;
}

.login-logo-wrap {
  flex-shrink: 0;
  overflow: hidden;
  line-height: 0;

  &--brand {
    width: 44px;
    height: 44px;
    border-radius: 12px;
    box-shadow:
      0 4px 16px rgba(0, 0, 0, 0.22),
      0 0 0 2px rgba(255, 255, 255, 0.35);
  }

  &--mobile {
    width: 40px;
    height: 40px;
    padding: 2px;
    border-radius: 10px;
    background: #fff;
    border: 1px solid rgba(0, 0, 0, 0.06);
    box-shadow: 0 2px 10px rgba(var(--el-color-primary-rgb), 0.14);
  }
}

.login-logo {
  display: block;
  width: 100%;
  height: 100%;
  border-radius: 10px;
  object-fit: cover;

  .login-logo-wrap--brand & {
    border-radius: 12px;
  }
}

.login-mobile-brand {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 10px;
  margin-bottom: 16px;
}

.brand-text {
  color: var(--el-text-color-primary);
}

.welcome-title {
  color: var(--el-text-color-regular);
  letter-spacing: 2px;
}

.welcome-subtitle {
  color: var(--el-text-color-primary);
}

.welcome-desc {
  color: var(--el-text-color-regular);
  opacity: 0.85;
}

.feature-tag {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding: 6px 14px;
  font-size: 13px;
  font-weight: 500;
  color: var(--el-text-color-regular);
  background: rgba(var(--el-color-primary-rgb), 0.08);
  border: 1px solid rgba(var(--el-color-primary-rgb), 0.15);
  border-radius: 20px;
  backdrop-filter: blur(8px);
  transition: all 0.3s;

  &:hover {
    background: rgba(var(--el-color-primary-rgb), 0.15);
    transform: translateY(-2px);
  }
}

.feature-tag-icon {
  color: var(--el-color-primary);
}

html.dark .feature-tag {
  background: rgba(255, 255, 255, 0.06);
  border-color: rgba(255, 255, 255, 0.1);
}

/* 右侧表单区 */
.login-form-side {
  position: relative;
  background: var(--el-bg-color);
  border-left: 1px solid var(--el-border-color-lighter);
}

html.dark .login-form-side {
  background: #0c0c0c;
  border-left-color: rgba(255, 255, 255, 0.06);
}

html.dark {
  .login-logo-wrap--brand {
    box-shadow:
      0 4px 16px rgba(0, 0, 0, 0.3),
      0 0 0 2px rgba(255, 255, 255, 0.2);
  }

  .login-logo-wrap--mobile {
    background: #fff;
    border-color: rgba(255, 255, 255, 0.1);
    box-shadow: 0 2px 12px rgba(0, 0, 0, 0.2);
  }
}

.login-form-panel {
  padding: 40px 24px;
}

.login-form {
  width: 100%;
  max-width: 300px;
}

.login-input {
  :deep(.el-input__wrapper) {
    border: 1px solid var(--el-border-color-lighter);
    border-radius: 10px;
    background: var(--el-fill-color-blank);
    box-shadow: none;
    transition: all 0.3s;

    &:hover,
    &.is-focus {
      border-color: var(--el-color-primary);
      box-shadow: 0 0 0 3px rgba(var(--el-color-primary-rgb), 0.1);
    }
  }
}

.login-form {
  :deep(.el-form-item.is-error .el-input__wrapper) {
    border-color: color-mix(in srgb, var(--el-color-danger) 65%, var(--el-border-color));
    box-shadow: none;

    &:hover,
    &.is-focus {
      border-color: var(--el-color-danger);
      box-shadow: none;
    }
  }
}

.login-verify-img {
  transition:
    transform 0.3s,
    box-shadow 0.3s,
    border-color 0.3s;

  &:hover {
    border-color: var(--el-color-primary) !important;
    box-shadow: 0 2px 8px rgba(var(--el-color-primary-rgb), 0.2);
    transform: scale(1.02);
  }
}

.login-btn {
  height: 44px;
  font-size: 16px;
  font-weight: 600;
  border-radius: 10px;
  box-shadow: 0 4px 14px rgba(var(--el-color-primary-rgb), 0.35);
  transition: all 0.3s;

  &:hover {
    transform: translateY(-2px);
    box-shadow: 0 6px 20px rgba(var(--el-color-primary-rgb), 0.45);
  }

  &:active {
    transform: translateY(0);
  }
}

.animate-float-picture {
  animation: float-picture 5s ease-in-out infinite;
  filter: drop-shadow(0 20px 40px rgba(var(--el-color-primary-rgb), 0.15));
}

@keyframes float-picture {
  0%,
  100% {
    transform: translateY(0);
  }
  50% {
    transform: translateY(-16px);
  }
}

@keyframes bg-float {
  0%,
  100% {
    transform: translate(0, 0) scale(1);
  }
  50% {
    transform: translate(15px, -10px) scale(1.05);
  }
}
.setup-hint {
  font-size: 12px;
  line-height: 1.8;
  color: var(--el-text-color-secondary);
  overflow-wrap: anywhere;
}
</style>
