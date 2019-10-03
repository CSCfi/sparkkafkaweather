FROM scholzj/centos-java-base:latest

ARG version=latest
ENV VERSION ${version}

COPY ./scripts/ /bin
RUN chmod a+x /bin/run.sh

ADD Sparkweather-0.0.1.jar /bin/

CMD ["/bin/run.sh", "/bin/Sparkweather-0.0.1.jar"]
