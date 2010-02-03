package genesequencing;

import ibis.cohort.Cohort;
import ibis.cohort.CohortFactory;
import ibis.cohort.MessageEvent;
import ibis.cohort.SingleEventCollector;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeSet;
import java.util.zip.GZIPOutputStream;

import neobio.alignment.ScoringScheme;

public class Dsearch {

    private final String inputFile; 
    private final Cohort cohort;
    private final boolean dump;
    
    public Dsearch(Cohort cohort, String inputFile, boolean dump) {
        this.cohort = cohort;
        this.inputFile = inputFile;
        this.dump = dump;        
    }

    public static void printMemStats(String prefix) {
        if(true) {
            Runtime r = Runtime.getRuntime();

            System.gc();
            long free = r.freeMemory() / (1024*1024);
            long max = r.maxMemory() / (1024*1024);
            long total = r.totalMemory() / (1024*1024);
            System.err.println(prefix + ": free = " + free + " max = " + max
                    + " total = " + total);
        }
    }

    private void saveResult(String inputFileName, ArrayList<ResSeq> result) {
        try {
            File tmp = new File(inputFileName);
            FileOutputStream fos = new FileOutputStream("result_"
                    + tmp.getName() + ".gz");
            BufferedOutputStream buf = new BufferedOutputStream(fos);
            GZIPOutputStream zip = new GZIPOutputStream(buf);
            PrintStream psRes = new PrintStream(zip);

            for (int i = 0; i < result.size(); i++) {
                psRes.println((result.get(i)).toString() + "\n\n");
            }

            psRes.close();
        } catch (Exception e) {
            System.out.println("Exception in createResultFile(): "
                    + e.toString());
        }
    }

    /*
    public static ArrayList<ResSeq> createTrivialResult(WorkUnit workUnit,
            SharedData shared, int startQuery, int endQuery, int startDatabase,
            int EndDatabase) {

        ArrayList<Sequence> querySequences = getSequences(shared
                .getQuerySequences(), startQuery, endQuery);
        ArrayList<Sequence> databaseSequences = getSequences(shared
                .getDatabaseSequences(), startDatabase, EndDatabase);

        return createTrivialResult(workUnit.alignmentAlgorithm,
                workUnit.scoresOrAlignments, workUnit.scoringScheme,
                querySequences, databaseSequences, workUnit.maxScores);
    }
    */
    
    public static ArrayList<Sequence> getSequences(ArrayList<Sequence> seq,
            int start, int end) {
        ArrayList<Sequence> res = new ArrayList<Sequence>();

        for (int i = start; i < end; i++) {
            res.add(seq.get(i));
        }

        return res;
    }

    public static ArrayList<ResSeq> createTrivialResult(WorkUnit workUnit) {
        return createTrivialResult(workUnit.alignmentAlgorithm,
                workUnit.scoresOrAlignments, workUnit.scoringScheme,
                workUnit.querySequences, workUnit.databaseSequences,
                workUnit.maxScores);
    }

    private static ArrayList<ResSeq> createTrivialResult(
            String alignmentAlgorithm, int scoresOrAlignments,
            ScoringScheme scoringScheme, ArrayList<Sequence> querySequences,
            ArrayList<Sequence> databaseSequences, int maxScores) {
        //Dsearch_AlgorithmV1 dA = new Dsearch_AlgorithmV1();

        ArrayList<ResSeq> resultUnit = Dsearch_AlgorithmV1.processUnit(querySequences,
                databaseSequences, scoresOrAlignments, scoringScheme,
                alignmentAlgorithm, maxScores);
        return resultUnit;
    }

    public ArrayList<ResSeq> generateResult(WorkUnit workUnit) {
                
        System.out.println("using cohort divide and conquer implementation");
        
        SingleEventCollector a = new SingleEventCollector();
        cohort.submit(a);
        cohort.submit(new DivCon(a.identifier(), workUnit, 0, true));
        
        Result tmp = (Result) ((MessageEvent) a.waitForEvent()).message;  
        
        System.out.println("App done at " + System.currentTimeMillis());
        
        return tmp.result;
        
        /*
        ArrayList<ResSeq> result;

        if (implementationName.equals("dc")) {
            System.out.println("using divide and conquer implementation");
            DivCon dC = new DivCon();
            result = dC.spawn_splitSequences(workUnit);
            dC.sync();
        } else if (implementationName.equals("so")) {
            System.out.println("using shared objects implementation");
            SharedData sharedData = new SharedData(workUnit.querySequences,
                    workUnit.databaseSequences);
            sharedData.exportObject();

            int qSize = workUnit.querySequences.size();
            int dbSize = workUnit.databaseSequences.size();
            workUnit.databaseSequences = null;
            workUnit.querySequences = null;

            DivConSO so = new DivConSO();
            result = so.spawn_splitQuerySequences(workUnit, sharedData, 0,
                    qSize, 0, dbSize);
            so.sync();
        } else if (implementationName.equals("mw")) {
            System.out.println("using master worker implementation");
            MasterWorker mw = new MasterWorker();
            result = mw.generateResult(workUnit);
        } else {
            throw new Error("illegal implementation name");
        }
        
        return result;
         */
    }

    public static ArrayList<ResSeq> combineSubResults(
            ArrayList<ResSeq>[] subResults) {
        HashMap<String, ResSeq> map = new HashMap<String, ResSeq>();
        for (ArrayList<ResSeq> sub : subResults) {
            for (ResSeq res : sub) {
                processSubResults(res, map);
            }
        }
        return new ArrayList<ResSeq>(map.values());
    }

    private static void processSubResults(ResSeq resSeq,
            HashMap<String, ResSeq> main) {

        String name = resSeq.getQuerySequence().getSequenceName();
        TreeSet<Sequence> newDatabaseSeqs = resSeq.getDatabaseSequences();

        ResSeq entry = main.get(name);
        if (entry != null) {
            entry.updateDatabaseSequences(newDatabaseSeqs);
        } else {
            main.put(name, resSeq);
        }
    }

    public void start() {
        
        InputReader iR = null;
        printMemStats("start");
        
        try {
            iR = new InputReader(inputFile);
        } catch (Throwable e) {
            throw new Error("An error occurred: " + e.toString());
        }

        String alignmentAlgorithm = iR.getAlignmentAlgorithm();
        int scoresOrAlignments = iR.getScoresOrAlignments();
        ScoringScheme scoringScheme = iR.getScoringScheme();
        int maxScores = iR.getMaxScores();
        int threshold = iR.getValueOfThreshold();

        String queryFile = iR.getQueryFile();
        FileSequences querySequences = new FileSequences(queryFile);
        printMemStats("query loaded");

        String databaseFile = iR.getDatabaseFile();
        FileSequences databaseSequences = new FileSequences(databaseFile);
        printMemStats("database loaded");

        System.out.println(databaseSequences.size() + " database Sequences, "
                + querySequences.size() + " query sequences, "
                + "threshold " + threshold);
        
        System.out.println("maximum database sequence length = " + databaseSequences.maxLength()
                + ", maximum query sequence length = " + querySequences.maxLength());

        double startTime = System.currentTimeMillis();

        WorkUnit workUnit = new WorkUnit(alignmentAlgorithm,
                scoresOrAlignments, scoringScheme, querySequences.getSequences(),
                databaseSequences.getSequences(), maxScores, threshold);

        ArrayList<ResSeq> result = generateResult(workUnit);
        printMemStats("done");

        System.out.println("application genesequencing_" 
                + " took " + (System.currentTimeMillis() - startTime) / 1000.0
                + " sec");

        if (dump) {
            double start1 = System.currentTimeMillis();
            saveResult(inputFile, result);
            double end1 = System.currentTimeMillis() - start1;
            System.out.println("\nThe result has been printed in " + end1
                    / 1000.0 + " sec");            
        }
    }

    public static void main(String[] args) {

        if (args.length < 1) {
            throw new Error(
                    "Usage: java Dsearch <input file> <implementation name (dc, so, ...)> [-dump]");
        }

        String inputFileName = args[0];
        
        boolean dump = false;

        if (args.length == 2) {
            dump = args[1].equals("-dump");
        }
        
        try { 
            Cohort cohort = CohortFactory.createCohort(); 

            if (cohort.isMaster()) { 
                new Dsearch(cohort, inputFileName, dump).start();
            } 
            
            cohort.done();
        
        } catch (Exception e) {
            System.err.println("Oops: " + e);
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }
}
