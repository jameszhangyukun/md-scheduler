package com.md.scheduler.job.admin.dao;

import com.md.scheduler.job.admin.core.model.JobLogGlue;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;


@Mapper
public interface JobLogGlueDao {
	
	public int save(JobLogGlue JobLogGlue);
	
	public List<JobLogGlue> findByJobId(@Param("jobId") int jobId);

	public int removeOld(@Param("jobId") int jobId, @Param("limit") int limit);


	public int deleteByJobId(@Param("jobId") int jobId);
	
}
