FROM jayjanssen/docker-coffeescript

WORKDIR /bot
RUN npm install bluebird redis winston moment child_process redlock node-vault minimist

COPY src /bot/

CMD ash