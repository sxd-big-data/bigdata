package com.bigdata.config;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.ReflectionUtils;
import org.springframework.web.servlet.config.annotation.InterceptorRegistration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import lombok.RequiredArgsConstructor;
import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.oas.annotations.EnableOpenApi;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;

@RequiredArgsConstructor
@EnableOpenApi
@Configuration
public class SwaggerConfiguration implements WebMvcConfigurer {

    @Bean
    public Docket createRestApi() {
        return new Docket(DocumentationType.OAS_30)     //文档版本
        		.groupName("bigdata")
                .pathMapping("/")
                .enable(true)  //是否开启swagger
                .apiInfo(apiInfo())                     //将api的元信息设置为包含在json ResourceListing响应中。
                .select()                               //选择哪些接口作为swagger的doc发布
                .apis(RequestHandlerSelectors.basePackage("com.bigdata.controller"))
                .paths(PathSelectors.any())
                .build()
                .protocols(new HashSet<>(Arrays.asList("http", "https")))   //支持的通讯协议集合
                ;
    }
    
    @Bean
    public Docket createHadoopRestApi() {
        return new Docket(DocumentationType.OAS_30)     //文档版本
        		.groupName("hadoop")
                .pathMapping("/")
                .enable(true)  //是否开启swagger
                .apiInfo(apiInfo())                     //将api的元信息设置为包含在json ResourceListing响应中。
                .select()                               //选择哪些接口作为swagger的doc发布
                .apis(RequestHandlerSelectors.basePackage("com.bigdata.hadoop.controller"))
                .paths(PathSelectors.any())
                .build()
                .protocols(new HashSet<>(Arrays.asList("http", "https")))   //支持的通讯协议集合
                ;
    }
    
    @Bean
    public Docket createHiveRestApi() {
        return new Docket(DocumentationType.OAS_30)     //文档版本
        		.groupName("hive")
                .pathMapping("/")
                .enable(true)  //是否开启swagger
                .apiInfo(apiInfo())                     //将api的元信息设置为包含在json ResourceListing响应中。
                .select()                               //选择哪些接口作为swagger的doc发布
                .apis(RequestHandlerSelectors.basePackage("com.bigdata.hive.controller"))
                .paths(PathSelectors.any())
                .build()
                .protocols(new HashSet<>(Arrays.asList("http", "https")))   //支持的通讯协议集合
                ;
    }
    @Bean
    public Docket createSparkRestApi() {
        return new Docket(DocumentationType.OAS_30)     //文档版本
        		.groupName("spark")
                .pathMapping("/")
                .enable(true)  //是否开启swagger
                .apiInfo(apiInfo())                     //将api的元信息设置为包含在json ResourceListing响应中。
                .select()                               //选择哪些接口作为swagger的doc发布
                .apis(RequestHandlerSelectors.basePackage("com.bigdata.spark.controller"))
                .paths(PathSelectors.any())
                .build()
                .protocols(new HashSet<>(Arrays.asList("http", "https")))   //支持的通讯协议集合
                ;
    }
    
    @Bean
    public Docket createKettleRestApi() {
        return new Docket(DocumentationType.OAS_30)     //文档版本
        		.groupName("kettle")
                .pathMapping("/")
                .enable(true)  //是否开启swagger
                .apiInfo(apiInfo())                     //将api的元信息设置为包含在json ResourceListing响应中。
                .select()                               //选择哪些接口作为swagger的doc发布
                .apis(RequestHandlerSelectors.basePackage("com.bigdata.kettle.controller"))
                .paths(PathSelectors.any())
                .build()
                .protocols(new HashSet<>(Arrays.asList("http", "https")))   //支持的通讯协议集合
                ;
    }
 
    private ApiInfo apiInfo() {
        return new ApiInfoBuilder()
                .title("bigdata项目")
                .description("大数据分析")
                .version("V1.0")
                .build();
    }

    

    /**
     	* 通用拦截器排除swagger设置，所有拦截器都会自动加swagger相关的资源排除信息
     */
    @SuppressWarnings("unchecked")
    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        try {
            Field registrationsField = FieldUtils.getField(InterceptorRegistry.class, "registrations", true);
            List<InterceptorRegistration> registrations = (List<InterceptorRegistration>) ReflectionUtils.getField(registrationsField, registry);
            if (registrations != null) {
                for (InterceptorRegistration interceptorRegistration : registrations) {
                    interceptorRegistration
                            .excludePathPatterns("/swagger**/**")
                            .excludePathPatterns("/webjars/**")
                            .excludePathPatterns("/v3/**")
                            .excludePathPatterns("/doc.html");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}