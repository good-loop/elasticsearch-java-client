package com.winterwell.es;

import java.io.File;

import org.junit.Test;

import com.winterwell.bob.tasks.EclipseClasspath;
import com.winterwell.bob.tasks.MavenDependencyTask;
import com.winterwell.utils.io.FileUtils;

public class UpdateEclipseClasspathTaskTest {

	@Test
	public void testDoTask() throws Exception {
		EclipseClasspath eclipseClasspath = new EclipseClasspath(FileUtils.getWorkingDirectory());
		File syncDir = new File(eclipseClasspath.getProjectDir(), MavenDependencyTask.MAVEN_DEPENDENCIES_FOLDER);
		UpdateEclipseClasspathTask uect = new UpdateEclipseClasspathTask(eclipseClasspath, syncDir);
		uect.doTask();
	}

}
