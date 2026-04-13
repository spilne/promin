import { createMDX } from 'fumadocs-mdx/next';

const withMDX = createMDX();

/** @type {import('next').NextConfig} */
const isProd = process.env.NODE_ENV === 'production';

const config = {
  output: 'export',
  distDir: '../../.target/docs',
  basePath: isProd ? '/promin' : '',
  trailingSlash: true,
  reactStrictMode: true,
};

export default withMDX(config);
