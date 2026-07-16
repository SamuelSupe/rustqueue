FROM golang:1.26-alpine AS build
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -mod=vendor -trimpath -o /rustqueue-compat .

FROM alpine:3.22
COPY --from=build /rustqueue-compat /usr/local/bin/rustqueue-compat
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/rustqueue-compat"]
