document.documentElement.classList.add("reveal-ready");

const requestedTheme = new URLSearchParams(window.location.search).get("theme");
if (requestedTheme === "light" || requestedTheme === "dark") {
  document.documentElement.dataset.theme = requestedTheme;
}

const menuButton = document.querySelector(".menu-button");
const siteNav = document.querySelector(".site-nav");

if (menuButton && siteNav) {
  menuButton.addEventListener("click", () => {
    const isOpen = menuButton.getAttribute("aria-expanded") === "true";
    menuButton.setAttribute("aria-expanded", String(!isOpen));
    siteNav.classList.toggle("is-open", !isOpen);
  });

  siteNav.addEventListener("click", (event) => {
    if (event.target instanceof HTMLAnchorElement) {
      menuButton.setAttribute("aria-expanded", "false");
      siteNav.classList.remove("is-open");
    }
  });
}

const tabs = [...document.querySelectorAll('[role="tab"]')];

function activateTab(nextTab) {
  for (const tab of tabs) {
    const isActive = tab === nextTab;
    tab.setAttribute("aria-selected", String(isActive));
    tab.tabIndex = isActive ? 0 : -1;

    const panelId = tab.getAttribute("aria-controls");
    const panel = panelId ? document.getElementById(panelId) : null;
    if (panel) {
      panel.hidden = !isActive;
    }
  }
}

for (const tab of tabs) {
  tab.addEventListener("click", () => activateTab(tab));
  tab.addEventListener("keydown", (event) => {
    if (!["ArrowLeft", "ArrowRight", "Home", "End"].includes(event.key)) {
      return;
    }

    event.preventDefault();
    const index = tabs.indexOf(tab);
    let nextIndex = index;

    if (event.key === "ArrowLeft") {
      nextIndex = (index - 1 + tabs.length) % tabs.length;
    } else if (event.key === "ArrowRight") {
      nextIndex = (index + 1) % tabs.length;
    } else if (event.key === "Home") {
      nextIndex = 0;
    } else if (event.key === "End") {
      nextIndex = tabs.length - 1;
    }

    const nextTab = tabs[nextIndex];
    activateTab(nextTab);
    nextTab.focus();
  });
}

const copyButton = document.querySelector(".copy-button");
const copyStatus = document.querySelector(".copy-status");
let copyStatusTimer;

function showCopyStatus(message) {
  if (!copyStatus) {
    return;
  }

  copyStatus.textContent = message;
  copyStatus.classList.add("is-visible");
  window.clearTimeout(copyStatusTimer);
  copyStatusTimer = window.setTimeout(() => {
    copyStatus.classList.remove("is-visible");
  }, 2400);
}

if (copyButton) {
  copyButton.addEventListener("click", async () => {
    const targetId = copyButton.getAttribute("data-copy-target");
    const target = targetId ? document.getElementById(targetId) : null;
    const text = target?.textContent?.trim();

    if (!text) {
      showCopyStatus("Command unavailable.");
      return;
    }

    try {
      await navigator.clipboard.writeText(text);
      copyButton.textContent = "Copied";
      showCopyStatus("Helm command copied.");
      window.setTimeout(() => {
        copyButton.textContent = "Copy";
      }, 1800);
    } catch {
      showCopyStatus("Copy failed. Select the command manually.");
    }
  });
}

const revealItems = document.querySelectorAll("[data-reveal]");
const reduceMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;

if (reduceMotion || !("IntersectionObserver" in window)) {
  for (const item of revealItems) {
    item.classList.add("is-visible");
  }
} else {
  const observer = new IntersectionObserver(
    (entries) => {
      for (const entry of entries) {
        if (entry.isIntersecting) {
          entry.target.classList.add("is-visible");
          observer.unobserve(entry.target);
        }
      }
    },
    { rootMargin: "0px 0px -10% 0px", threshold: 0.12 },
  );

  for (const item of revealItems) {
    observer.observe(item);
  }
}
