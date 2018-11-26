package com.sixku.canal.mybatis;

import com.sixku.canal.mybatis.domain.OrderFlow;
import com.sixku.canal.mybatis.domain.OrderFlowExample;
import java.util.List;
import org.apache.ibatis.annotations.Param;

public interface OrderFlowMapper {
    long countByExample(OrderFlowExample example);

    int deleteByExample(OrderFlowExample example);

    int deleteByPrimaryKey(Long flowId);

    int insert(OrderFlow record);

    int insertSelective(OrderFlow record);

    List<OrderFlow> selectByExample(OrderFlowExample example);

    OrderFlow selectByPrimaryKey(Long flowId);

    int updateByExampleSelective(@Param("record") OrderFlow record, @Param("example") OrderFlowExample example);

    int updateByExample(@Param("record") OrderFlow record, @Param("example") OrderFlowExample example);

    int updateByPrimaryKeySelective(OrderFlow record);

    int updateByPrimaryKey(OrderFlow record);
}