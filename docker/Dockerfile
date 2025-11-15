FROM alpine:latest
RUN apk update && apk add openssh-client sshpass
WORKDIR /app
COPY start.sh .
RUN chmod +x start.sh
CMD ["./start.sh"]