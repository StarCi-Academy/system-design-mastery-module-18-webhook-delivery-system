# Multi-stage build (SELF-BUILD only — no cloud image, no DockerHub push).
# Stage 1: install production dependencies.
FROM node:20-alpine AS deps
WORKDIR /app
COPY package.json ./
RUN npm install --omit=dev

# Stage 2: runtime image with source + installed node_modules.
FROM node:20-alpine AS runtime
WORKDIR /app
ENV NODE_ENV=production
COPY --from=deps /app/node_modules ./node_modules
COPY package.json ./
COPY src ./src
COPY receiver ./receiver
EXPOSE 3000
# Default command runs the emitter; receiver-mock overrides via compose `command`.
CMD ["node", "src/index.js"]
