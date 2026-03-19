FROM node:20-slim@sha256:6c51af7dc83f4708aaac35991306bca8f478351cfd2bda35750a62d7efcf05bb

WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci --omit=dev

RUN groupadd -r app && useradd -r -g app -d /app app && \
    chown -R app:app /app

COPY --chown=app:app . .

USER app

EXPOSE 4000

CMD ["node", "src/index.js"]
