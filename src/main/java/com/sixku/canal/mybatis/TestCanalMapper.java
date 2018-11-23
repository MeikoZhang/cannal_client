package com.sixku.canal.mybatis;

import com.sixku.canal.mybatis.domain.TestCanal;
import com.sixku.canal.mybatis.domain.TestCanalExample;
import java.util.List;
import org.apache.ibatis.annotations.Param;

public interface TestCanalMapper {
    long countByExample(TestCanalExample example);

    int deleteByExample(TestCanalExample example);

    int deleteByPrimaryKey(Integer id);

    int insert(TestCanal record);

    int insertSelective(TestCanal record);

    List<TestCanal> selectByExample(TestCanalExample example);

    TestCanal selectByPrimaryKey(Integer id);

    int updateByExampleSelective(@Param("record") TestCanal record, @Param("example") TestCanalExample example);

    int updateByExample(@Param("record") TestCanal record, @Param("example") TestCanalExample example);

    int updateByPrimaryKeySelective(TestCanal record);

    int updateByPrimaryKey(TestCanal record);
}