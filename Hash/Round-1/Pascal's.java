class Solution {
    public static void printPascalsTriangle(int numRows) {
        int[][] triangle = new int[numRows][numRows];
        for (int i = 0; i < numRows; i++) {
            triangle[i][0] = 1;  
            triangle[i][i] = 1;
            for (int j = 1; j < i; j++) {
                triangle[i][j] = triangle[i - 1][j - 1] + triangle[i - 1][j];
            }
        }
        for (int i = 0; i < numRows; i++) {
            for (int j = 0; j < numRows - i - 1; j++) {
                System.out.print(" ");
            }
            for (int j = 0; j <= i; j++) {
                System.out.print(triangle[i][j] + " ");
            }
            System.out.println();
        }
    }
    public static void main(String[] args) {
        int numRows = 10;  
        printPascalsTriangle(numRows);
    }
}