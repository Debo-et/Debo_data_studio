// src/services/jobService.ts
export interface JobConfig {
  id: string;
  name: string;
  createdAt: Date;
  lastModified: Date;
  nodes: any[];
  connections: any[];
  variables: any[];
  metadata?: Record<string, any>;
}

export class JobService {
  static saveJob(job: JobConfig): void {
    try {
      const jobData = {
        ...job,
        lastModified: new Date()
      };
      localStorage.setItem(`job_${job.id}`, JSON.stringify(jobData));
      localStorage.setItem('current_job_id', job.id);
    } catch (error) {
      console.error('Failed to save job:', error);
    }
  }

  static loadJob(jobId: string): JobConfig | null {
    try {
      const data = localStorage.getItem(`job_${jobId}`);
      if (data) {
        return JSON.parse(data);
      }
    } catch (error) {
      console.error('Failed to load job:', error);
    }
    return null;
  }

  static deleteJob(jobId: string): void {
    localStorage.removeItem(`job_${jobId}`);
    if (localStorage.getItem('current_job_id') === jobId) {
      localStorage.removeItem('current_job_id');
    }
  }

  static listJobs(): JobConfig[] {
    const jobs: JobConfig[] = [];
    for (let i = 0; i < localStorage.length; i++) {
      const key = localStorage.key(i);
      if (key?.startsWith('job_')) {
        try {
          const job = JSON.parse(localStorage.getItem(key)!);
          jobs.push(job);
        } catch (error) {
          console.warn(`Failed to parse job ${key}:`, error);
        }
      }
    }
    return jobs.sort((a, b) => 
      new Date(b.lastModified).getTime() - new Date(a.lastModified).getTime()
    );
  }

  static createJob(name: string): JobConfig {
    const job: JobConfig = {
      id: `job_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
      name,
      createdAt: new Date(),
      lastModified: new Date(),
      nodes: [],
      connections: [],
      variables: []
    };
    return job;
  }
}