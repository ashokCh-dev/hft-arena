# Sandbox template: C++ submission.
# The orchestrator drops the contestant's source in as engine.cpp before building.
# crow.h (header-only HTTP/WebSocket/JSON) is provided by the platform.
FROM gcc:13 AS build
WORKDIR /src
RUN apt-get update && apt-get install -y --no-install-recommends libasio-dev \
    && rm -rf /var/lib/apt/lists/*
COPY crow.h /src/crow.h
COPY engine.cpp /src/engine.cpp
# Standalone Asio (no Boost): crow selects <asio.hpp> unless CROW_USE_BOOST is
# *defined*, so we must not pass -DCROW_USE_BOOST at all.
# Static libstdc++/libgcc so the slim runtime image needs no matching GLIBCXX.
RUN g++ -std=c++17 -O3 -pthread -DASIO_STANDALONE \
        -static-libstdc++ -static-libgcc engine.cpp -o /src/engine

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends libstdc++6 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -m runner
COPY --from=build /src/engine /usr/local/bin/engine
EXPOSE 9000
USER runner
CMD ["engine"]
