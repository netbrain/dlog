FROM golang:1.4

ENV APPNAME dlog
ENV APPPATH src/github.com/netbrain/${APPNAME}


RUN mkdir -p ${APPPATH}
ADD . ${APPPATH}
WORKDIR ${APPPATH}

RUN make
EXPOSE 8080
ENTRYPOINT ["dlog"]