package com.sixku.canal.util;

import com.alibaba.fastjson.JSON;
import com.sixku.canal.mybatis.TestCanalMapper;
import com.sixku.canal.mybatis.domain.TestCanal;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

import java.io.IOException;
import java.io.InputStream;

import static org.apache.ibatis.io.Resources.getResourceAsStream;

/**
 * Mybatis获取session类
 */
public class MyBatisUtil {

    static SqlSessionFactory sqlSessionFactory;

    static {
        String resource = "mybatis-config.xml";
        InputStream inputStream = null;
        try {
            inputStream = getResourceAsStream(resource);
            sqlSessionFactory =
                    new SqlSessionFactoryBuilder().build(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getConfig(String key){
        return sqlSessionFactory.getConfiguration().getVariables().getProperty(key);
    }

    public static Integer getIntegerConfig(String key){
        return Integer.valueOf(sqlSessionFactory.getConfiguration().getVariables().getProperty(key));
    }

    public static String getConfig(String key,String defaultValue){
        return sqlSessionFactory.getConfiguration().getVariables().getProperty(key,defaultValue);
    }


    public static SqlSession getSqlSession(){
        return sqlSessionFactory.openSession();
    }

    public static void main(String args[]) {
//        SqlSession sqlSession = getSqlSession();
//        TestCanalMapper mapper = sqlSession.getMapper(TestCanalMapper.class);
//        System.out.println(mapper);
//        TestCanal testCanal = mapper.selectByPrimaryKey(1);
//        sqlSession.commit();
//        System.out.println(testCanal);

        Configuration configuration = sqlSessionFactory.getConfiguration();
        System.out.println(JSON.toJSONString(configuration.getVariables()));
    }
}
