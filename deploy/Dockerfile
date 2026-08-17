# The UI is built first and embedded into the Go binary, so the runtime image
# is one static binary with no assets beside it and nothing to serve them with.

FROM node:20-alpine AS ui
WORKDIR /src/web
COPY web/package.json web/package-lock.json* ./
RUN npm install --no-audit --no-fund
COPY web/ ./
RUN npm run build

FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# The UI build output has to be in place before the embed directive is compiled.
COPY --from=ui /src/internal/ui/dist ./internal/ui/dist
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/orchd ./cmd/orchd \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/orchctl ./cmd/orchctl

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/orchd /orchd
COPY --from=build /out/orchctl /orchctl
EXPOSE 9443
ENTRYPOINT ["/orchd"]
