# Java 8 için uygun bir base image kullanıyoruz
FROM openjdk:8-jdk-alpine

# Çalışma dizinini oluştur
WORKDIR /app

# Spring Boot JAR dosyasını image içine kopyala
COPY target/*.jar app.jar

# Uygulamayı çalıştır
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
