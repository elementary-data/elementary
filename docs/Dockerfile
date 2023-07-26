FROM node:19

WORKDIR /app
RUN npm i -g mintlify
RUN mintlify install

EXPOSE 3000
CMD ["mintlify", "dev"]
