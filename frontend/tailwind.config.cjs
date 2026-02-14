/** @type {import('tailwindcss').Config} */
module.exports = {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      // Keep Figma’s exact colors available.
      colors: {
        oaktree: {
          green: "#14312c",
          green2: "#3a524e",
          sand: "#efecdc",
          paper: "#fffbea"
        }
      }
    }
  },
  plugins: []
};
