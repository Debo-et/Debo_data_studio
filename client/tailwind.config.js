/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        border: "hsl(var(--border))",
        input: "hsl(var(--input))",
        ring: "hsl(var(--ring))",
        background: "hsl(var(--background))",
        foreground: "hsl(var(--foreground))",
        primary: {
          DEFAULT: "hsl(var(--primary))",
          foreground: "hsl(var(--primary-foreground))",
        },
        secondary: {
          DEFAULT: "hsl(var(--secondary))",
          foreground: "hsl(var(--secondary-foreground))",
        },
        destructive: {
          DEFAULT: "hsl(var(--destructive))",
          foreground: "hsl(var(--destructive-foreground))",
        },
        muted: {
          DEFAULT: "hsl(var(--muted))",
          foreground: "hsl(var(--muted-foreground))",
        },
        accent: {
          DEFAULT: "hsl(var(--accent))",
          foreground: "hsl(var(--accent-foreground))",
        },
        popover: {
          DEFAULT: "hsl(var(--popover))",
          foreground: "hsl(var(--popover-foreground))",
        },
        card: {
          DEFAULT: "hsl(var(--card))",
          foreground: "hsl(var(--card-foreground))",
        },
        sidebar: {
          DEFAULT: "hsl(var(--sidebar-background))",
          foreground: "hsl(var(--sidebar-foreground))",
          primary: "hsl(var(--sidebar-primary))",
          "primary-foreground": "hsl(var(--sidebar-primary-foreground))",
          accent: "hsl(var(--sidebar-accent))",
          "accent-foreground": "hsl(var(--sidebar-accent-foreground))",
          border: "hsl(var(--sidebar-border))",
          ring: "hsl(var(--sidebar-ring))"
        },
        canvas: {
          DEFAULT: "hsl(var(--canvas-background))",
          foreground: "hsl(var(--canvas-foreground))",
        },
        console: {
          DEFAULT: "hsl(var(--console-background))",
          foreground: "hsl(var(--console-foreground))",
          header: "hsl(var(--console-header))",
          border: "hsl(var(--console-border))",
        }
      },
      borderRadius: {
        lg: "var(--radius)",
        md: "calc(var(--radius) - 2px)",
        sm: "calc(var(--radius) - 4px)",
      },
      animation: {
        'pulse-slow': 'pulse 2s cubic-bezier(0.4, 0, 0.6, 1) infinite',
        'fade-in': 'fade-in 0.3s ease-out',
      },
      keyframes: {
        'fade-in': {
          '0%': { opacity: '0', transform: 'translateY(-10px)' },
          '100%': { opacity: '1', transform: 'translateY(0)' }
        }
      },
      // NEW: Add spacing utilities for 25% scaled nodes
      spacing: {
        '1.25': '0.3125rem',   // 5px (1.25 * 4 = 5)
        '2.5': '0.625rem',     // 10px (2.5 * 4 = 10)
        '3.75': '0.9375rem',   // 15px (3.75 * 4 = 15)
        '17.5': '4.375rem',    // 70px / 4 = 17.5px
        '22.5': '5.625rem',    // 90px / 4 = 22.5px
      },
      // NEW: Add min-width utilities
      minWidth: {
        '25': '6.25rem',       // 100px / 4 = 25px
      },
      // NEW: Add min-height utilities
      minHeight: {
        '17.5': '4.375rem',    // 70px / 4 = 17.5px
      },
      // NEW: Add font size for scaled node labels
      fontSize: {
        'xxs': '0.625rem',     // 10px for scaled node labels
      },
      // NEW: Add more precise spacing for scaled UI
      width: {
        '2.5': '0.625rem',     // 10px for small containers
        '3.75': '0.9375rem',   // 15px
      },
      height: {
        '2.5': '0.625rem',     // 10px for small containers
        '3.75': '0.9375rem',   // 15px
      },
      // NEW: Add border width for scaled handles
      borderWidth: {
        '1.5': '1.5px',
      },
      // NEW: Add margin utilities for scaled spacing
      margin: {
        '0.25': '0.0625rem',   // 1px (0.25 * 4 = 1)
        '0.5': '0.125rem',     // 2px
        '0.75': '0.1875rem',   // 3px
      },
      padding: {
        '1.5': '0.375rem',     // 6px for scaled node padding
        '0.5': '0.125rem',     // 2px
        '0.75': '0.1875rem',   // 3px
      }
    },
  },
  plugins: [],
}