/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{js,jsx,ts,tsx}"],
  darkMode: "class",
  theme: {
    extend: {
      colors: {
        hermes: {
          bg: "#090d16",
          panel: "#111827",
          border: "#1f2937",
          accent: "#3b82f6",
          low: "#22c55e",
          medium: "#f59e0b",
          high: "#ef4444",
          critical: "#a855f7",
        },
      },
    },
  },
  plugins: [],
};
