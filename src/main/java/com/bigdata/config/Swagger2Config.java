/*
 * package com.bigdata.config;
 * 
 * import org.springframework.context.annotation.Bean; import
 * org.springframework.context.annotation.Configuration;
 * 
 * import springfox.documentation.builders.ApiInfoBuilder; import
 * springfox.documentation.builders.PathSelectors; import
 * springfox.documentation.builders.RequestHandlerSelectors; import
 * springfox.documentation.service.ApiInfo; import
 * springfox.documentation.service.Contact; import
 * springfox.documentation.spi.DocumentationType; import
 * springfox.documentation.spring.web.plugins.Docket;
 * 
 * import com.google.common.base.Function; import
 * com.google.common.base.Optional; import com.google.common.base.Predicate;
 * 
 * import springfox.documentation.RequestHandler; import
 * springfox.documentation.swagger2.annotations.EnableSwagger2;
 * 
 * @SuppressWarnings("all")
 * 
 * @Configuration
 * 
 * @EnableSwagger2//开启 public class Swagger2Config {
 * 
 * private static final String splitor = ";";
 * 
 * @Bean public Docket createRestApi() { String basePackages =
 * "com.bigdata.hive.controller" + splitor + "com.bigdata.hadoop.controller"+
 * splitor + "com.bigdata.kettle.controller" + splitor +
 * "com.bigdata.spark.controller"; return new
 * Docket(DocumentationType.SWAGGER_2) .apiInfo(apiInfo()) .select()
 * .apis(basePackage(basePackages)) .paths(PathSelectors.any()) .build();
 * 
 * }
 * 
 * 
 *//**
	 * 声明基础包
	 * 
	 * @param basePackage 基础包路径
	 * @return
	 */
/*
 * public static Predicate<RequestHandler> basePackage(final String basePackage)
 * { return new Predicate<RequestHandler>() {
 * 
 * @Override public boolean apply(RequestHandler input) { return
 * declaringClass(input).transform(handlerPackage(basePackage)).or(true); } }; }
 *//**
	 * 校验基础包
	 * 
	 * @param basePackage 基础包路径
	 * @return
	 */
/*
 * private static Function<Class<?>, Boolean> handlerPackage(final String
 * basePackage) { return input -> { for (String strPackage :
 * basePackage.split(splitor)) { boolean isMatch =
 * input.getPackage().getName().startsWith(strPackage); if (isMatch) { return
 * true; } } return false; }; }
 * 
 *//**
	 * 检验基础包实例
	 * 
	 * @param requestHandler 请求处理类
	 * @return
	 *//*
		 * @SuppressWarnings("deprecation") private static Optional<? extends Class<?>>
		 * declaringClass(RequestHandler requestHandler) { return
		 * Optional.fromNullable(requestHandler.declaringClass()); }
		 * 
		 * private ApiInfo apiInfo() { return new ApiInfoBuilder() .title("hive接口")
		 * .description("大数据操作") .contact(new Contact("gaodongjie@126.com", null, null))
		 * .version("V1.0") .build(); } }
		 */