FROM node:20-slim

WORKDIR /app

COPY package.json ./
RUN npm install

COPY . .

ENV NODE_ENV=development
ENV PORT=4000

EXPOSE 4000

CMD ["npm", "run", "dev"]
