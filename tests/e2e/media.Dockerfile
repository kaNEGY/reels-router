ARG MEDIA_IMAGE
FROM ${MEDIA_IMAGE}
USER root
COPY tests/e2e/fake_yt_dlp.py /usr/local/bin/yt-dlp
RUN chmod 755 /usr/local/bin/yt-dlp
USER 10001
