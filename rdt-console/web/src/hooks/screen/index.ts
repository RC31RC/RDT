import { useMediaQuery } from "@vueuse/core";
export function useScreenStore() {
  return { isMobile: useMediaQuery("(max-width: 767px)") };
}
