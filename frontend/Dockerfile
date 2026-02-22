FROM node:20-alpine AS build

WORKDIR /app

COPY package*.json /app/
RUN npm install

COPY . /app

ARG VITE_API_BASE=
ARG VITE_BASE_URL=
ARG VITE_BACKEND_PORT=8000
ENV VITE_API_BASE=${VITE_API_BASE}
ENV VITE_BASE_URL=${VITE_BASE_URL}
ENV VITE_BACKEND_PORT=${VITE_BACKEND_PORT}

RUN npm run build

FROM node:20-alpine

WORKDIR /app

RUN npm install -g serve
COPY --from=build /app/dist /app/dist

EXPOSE 5173

CMD ["serve", "-s", "dist", "-l", "5173"]
