
package com.winterwell.es;

import java.util.ArrayList;
import java.util.List;

import com.winterwell.bob.BuildTask;
import com.winterwell.bob.tasks.MavenDependencyTask;
import com.winterwell.bob.wwjobs.BuildWinterwellProject;

public class BuildESJavaClient extends BuildWinterwellProject {

	public BuildESJavaClient() {
		super("elasticsearch-java-client");
		setIncSrc(true);
		setVersion("1.0.1-ES7");  // 1 June 2021
	}

	@Override
	public List<BuildTask> getDependencies() {
		List<BuildTask> deps = new ArrayList(super.getDependencies());
				
		MavenDependencyTask mdt = new MavenDependencyTask();
		mdt.addDependency("com.google.guava", "guava", "30.1-jre");
		mdt.setIncSrc(true);		
		deps.add(mdt);

		// TODO
//		File depdir = new File(projectDir, MavenDependencyTask.MAVEN_DEPENDENCIES_FOLDER);
//		UpdateEclipseClasspathTask uect = new UpdateEclipseClasspathTask(
//				new EclipseClasspath(projectDir), depdir);		
//		deps.add(uect);
		
		return deps;
	}
	
	@Override
	public void doTask() throws Exception {
		super.doTask();		
	}

}
