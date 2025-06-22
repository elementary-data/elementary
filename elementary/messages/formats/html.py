from elementary.messages.blocks import Icon

ICON_TO_HTML = {
    Icon.RED_TRIANGLE: "🔺",
    Icon.X: "❌",
    Icon.WARNING: "⚠️",
    Icon.EXCLAMATION: "❗",
    Icon.CHECK: "✅",
    Icon.MAGNIFYING_GLASS: "🔎",
    Icon.HAMMER_AND_WRENCH: "🛠️",
    Icon.POLICE_LIGHT: "🚨",
    Icon.INFO: "ℹ️",
    Icon.EYE: "👁️",
    Icon.GEAR: "⚙️",
    Icon.BELL: "🔔",
    Icon.GEM: "💎",
    Icon.SPARKLES: "✨",
}

for icon in Icon:
    if icon not in ICON_TO_HTML:
        raise RuntimeError(f"No HTML representation for icon {icon}")
