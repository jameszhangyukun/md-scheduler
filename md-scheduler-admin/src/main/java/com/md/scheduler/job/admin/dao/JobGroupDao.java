package com.md.scheduler.job.admin.dao;

import com.md.scheduler.job.admin.core.model.JobGroup;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;


@Mapper
public interface JobGroupDao {

    public List<JobGroup> findAll();

    public List<JobGroup> findByAddressType(@Param("addressType") int addressType);

    public int save(JobGroup JobGroup);

    public int update(JobGroup JobGroup);

    public int remove(@Param("id") int id);

    public JobGroup load(@Param("id") int id);

    public List<JobGroup> pageList(@Param("offset") int offset,
                                   @Param("pagesize") int pagesize,
                                   @Param("appname") String appname,
                                   @Param("title") String title);

    public int pageListCount(@Param("offset") int offset,
                             @Param("pagesize") int pagesize,
                             @Param("appname") String appname,
                             @Param("title") String title);

}
