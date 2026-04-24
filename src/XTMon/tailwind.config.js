module.exports = {
  darkMode: "class",
  content: ["./Components/**/*.{razor,html}", "./**/*.razor"],
  safelist: ["submenu-status-badge--warning"],
  theme: {
    extend: {
      colors: {
        ink: {
          900: "#0f172a"
        }
      }
    }
  },
  plugins: []
};
