/**
 * Instrument-panel palette.
 *
 * Pools are the primary data dimension in this system, so pool identity gets
 * the categorical hues and everything else stays achromatic. Heat is the only
 * other thing allowed colour, on its own ramp, because a throttling card is the
 * one reading an operator must never mistake for a healthy one.
 */
export default {
  content: ['./index.html', './src/**/*.{js,jsx}'],
  theme: {
    extend: {
      colors: {
        ground: '#12152a',
        panel: '#191d36',
        raised: '#212747',
        hairline: '#2e3763',
        ink: '#e6e9f7',
        muted: '#8b94bd',
        dim: '#5b6491',
        pool: {
          1: '#3ad4e0',
          2: '#f5a83c',
          3: '#a98bff',
          4: '#ff7a8a',
          5: '#a6d95b',
        },
        heat: {
          cool: '#4fb8f5',
          warm: '#f5a83c',
          hot: '#ff5d5d',
        },
      },
      fontFamily: {
        mono: ['ui-monospace', 'SFMono-Regular', 'SF Mono', 'Cascadia Mono',
               'Menlo', 'Consolas', 'monospace'],
        sans: ['ui-sans-serif', 'system-ui', '-apple-system', 'Segoe UI',
               'Roboto', 'Helvetica Neue', 'sans-serif'],
      },
      fontSize: {
        micro: ['0.625rem', { lineHeight: '0.875rem', letterSpacing: '0.12em' }],
        tiny: ['0.6875rem', { lineHeight: '1rem' }],
      },
      keyframes: {
        // Borrowed capacity drifts. Owned capacity is still. Reclaim is
        // visible as the motion stopping.
        drift: {
          '0%': { backgroundPosition: '0 0' },
          '100%': { backgroundPosition: '28px 0' },
        },
        pulse_ring: {
          '0%,100%': { opacity: '0.35' },
          '50%': { opacity: '1' },
        },
      },
      animation: {
        drift: 'drift 1.4s linear infinite',
        'pulse-ring': 'pulse_ring 1.8s ease-in-out infinite',
      },
    },
  },
  plugins: [],
}
